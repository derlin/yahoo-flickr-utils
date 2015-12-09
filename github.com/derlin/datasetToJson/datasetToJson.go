package main;

import (
    "net/http"
    "fmt"
    "encoding/json"
    "encoding/xml"
    "net/url"
    "io/ioutil"
    "os"
    "bufio"
    "errors"
    "strings"
    "log"
    "math"
    "strconv"
    "time"
)


const (
    SEARCH_URL = "https://api.flickr.com/services/rest/?method=flickr.photos.search&api_key=243075ed07d2c7f3c7ea7a408db48c62"
    SEARCH_EXTRAS = "description,tags,machine_tags,url_o,geo,date_upload,date_taken,owner_name"
)

// ------------------------------------------------

type Response struct {
    Photos []Photo `xml:"photos>photo"`
}


type Photo struct {
    Id string       `xml:"id,attr" json:"id"`
    Owner string    `xml:"owner,attr" json:"owner"`
    Secret string   `xml:"secret,attr" json:"secret"`
    Server string   `xml:"server,attr" json:server"`
    Farm int        `xml:"farm,attr" json:"farm"`
    Title string    `xml:"title,attr" json:"title"`
    Descr string    `xml:"description" json:"description"`

    Tags string               `xml:"tags,attr" json:"-"`
    TagsArray []string        `json:"tags"`
    MachineTags string        `xml:"machine_tags,attr" json:"-"`
    MachineTagsArray []string `json:"machine_tags"`

    Url string      `xml:"url_o,attr" json:"url"`
    Height int      `xml:"height_o,attr" json:"height"`
    Width int       `xml:"width_o,attr" json:"width"`

    GeoInfos        `json:"geo"`
    DatesInfos      `json:"dates"`
}

type DatesInfos struct {  
    Upload string    `xml:"dateupload,attr" json:"upload"`
    Taken string     `xml:"datetaken,attr" json:"taken"`
}

type GeoInfos struct {   
    Latitude int    `xml:"latitude,attr" json:"lat"`
    Longitude int   `xml:"longitude,attr" json:"long"`
    Accuracy int   `xml:"accuracy,attr" json:"accuracy"`
    Context int    `xml:"context,attr" json:"context"`
}

type JsonResult struct {
    Id string
    Json string
}


// ------------------------------------------------

var logger *log.Logger


func ExitErr(info string) {
    log.Println(info)
    os.Exit(1)
}

func CheckErr(err error) {
    if err != nil {
        ExitErr(fmt.Sprintf("%s\n",err))
    }
}

// ------------------------------------------------
func timestampToSqlDate(timestamp string) (string, error) {
    i, err := strconv.ParseInt(timestamp, 10, 64)
    if err != nil {
        return "", err
    }
    tm := time.Unix(i, 0)
    s := tm.Format(time.RFC3339)
    s = strings.Replace(s, "T", " ", -1)[:len(s)-1]
    return s, nil
}
// ------------------------------------------------


func main(){
    logger = log.New(os.Stderr, "logger: ", log.Lshortfile)

    if len(os.Args) < 2 {
        ExitErr(fmt.Sprintf("usage: %s <path to data file> [<nbr processes>]\n", os.Args[0]))
    }

    nbrProcesses := 2 // default 

    if len(os.Args) > 2 {
        
        var err error

        if nbrProcesses, err = strconv.Atoi(os.Args[2]); err != nil {
            ExitErr(fmt.Sprintf("error: %s is not an int", os.Args[2]))
        } 
        
        if nbrProcesses < 0 || nbrProcesses > 50 {
            ExitErr("error: nbrProcesses must be between 1 and 50")
        }

    }
    
    dispatcher(os.Args[1], nbrProcesses)
}

// ------------------------------------------------
//go run datasetToJson.go F:\dataset0-split-500_000\yfcc100m_dataset-002 20 1> yfcc100m_dataset-002.json 2> yfcc100m_dataset-002.log


/**
 * @brief The dispatcher launches the processes and wait for the reducer to finish
 * 
 * @param string the filepath to the yahoo-flickr-datafile to process
 * @param int the number of processes/mappers to use
 */
func dispatcher(filepath string, nbrProcesses int){

    in := make(chan string)
    outOk := make(chan *JsonResult)
    outError := make(chan error)


    // launch processes
    for i := 0; i < nbrProcesses; i++ {
        go mapper(i, in, outOk, outError)
    }

    // launch reducer
    totalLines := make(chan int64)
    done := make(chan bool)
    go reducer(outOk, outError, totalLines, done)

    // read file and dispatch lines to processes
    file, err := os.Open(os.Args[1])
    CheckErr(err)
    scanner := bufio.NewScanner(file)
    var lines int64

    for scanner.Scan() {
        in <- scanner.Text()
        //log.Println("Master: dispatched 1 line")
        lines++
    }
    totalLines <- lines
    close(totalLines)

    if err := scanner.Err(); err != nil {
        CheckErr(err)
    } 

    file.Close()
    log.Println("Master: dispatched jobs")

    close(in)
    // wait for the reducer to be done
    <- done

}


/**
 * @brief Gather the results from the mapper
 * @details Prints out the results and/or errors, i.e. the json associated with each image from the input file
 * 
 * @param chan Channel used to receive the json results from the mappers
 * @param chan Channel used to receive the errors from the mappers
 * @param chan Channel used to receive the total number of lines from the dispatcher (to know when to stop)
 * @param chan Channel used to send a signal to the dispatcher when done
 */
func reducer(outOk chan *JsonResult, outError chan error, totalLines chan int64, done chan bool ) {
    // gather results
    var totalOks, totalErrors, results int64
    
    var lines, l int64
    lines =  math.MaxInt64

    for results = 0; results < lines; {
        var ok bool

        select {
            case l, ok = <- totalLines:
                if ok { 
                    lines = l
                    log.Printf("Reducer: total lines received: %d\n", lines)
                }

            case err, ok := <- outError:
                if ok { 
                    log.Println(err)
                    totalErrors++
                    results++
                }

            case res, ok := <- outOk:
                if ok {
                    fmt.Println(res.Json)
                    log.Println(res.Id + " : OK")
                    totalOks++
                    results++
                }

            default:
                ;
        }
    }

    // close channels
    close(outOk)
    close(outError)

    // finish
    log.Printf("STATS: lines = %d, ok = %d, errors = %d\n", lines, totalOks, totalErrors)
    done <- true
}


/**
 * @brief A mapper processes one line from the input file, fetches the details
 * related to the image from Flickr and returns an error or a json.
 * 
 * @param int unique id of the process (for logging purposes)
 * @param chan the input channel, one line at a time
 * @param chan the channel used to send the results (json) to the reducer
 * @param chan the channel used to send the errors to the reducer
 */
func mapper(jid int, in chan string, outOk chan *JsonResult, outError chan error) {

    log.Printf("Job %d starting\n", jid)

    for {

        line, ok := <- in
        if !ok {
            log.Printf("Job %d finished\n", jid)
            return
        }

        //log.Printf("Job %d processing one line\n", jid)
        tokens := strings.Fields(line)
        id := tokens[0]
        owner := tokens[1]
        dateTaken := tokens[3] + " " + tokens[4][:len(tokens[4])-2]
        j, err := getJson(id, owner, dateTaken)

        if err != nil {
            outError <- err

        }else{
            outOk <- &JsonResult{id, string(j)}
        }

        //log.Printf("Job %d processed one line\n", jid)
    }

}

// ------------------------------------------------

/**
 * @brief Fetches the details about an image from Flickr and returns a json
 * @details only images with tags are returned.
 * 
 * @param string id of the image
 * @param string owner id (nsid) of the owner
 * @param string date taken, in sql.date format
 * @return a json representation of the Photo struct, or an error if 
 * the image has not been found or the taglist is empty
 */
func getJson(id string, owner string, dateTaken string) (string, error) {
    ps, err := photoSearch(owner, dateTaken)

    if err != nil {
        return "", errors.New(fmt.Sprintf("%s %s %s : not found (%s)", id, owner, dateTaken, err))
    }


    var p Photo
    for _, photo := range ps {
        if photo.Id == id {
            p = photo
        }
    }  


    if p.Id == "" {
        return "", errors.New(fmt.Sprintf("%s %s %s : not found (%s)", id, owner, dateTaken))
    }

    if p.Tags == "" {
        return "", errors.New(fmt.Sprintf("%s [%s] : tags lists empty", id, p.Url))
    }

    p.TagsArray = strings.Fields(p.Tags)
    p.MachineTagsArray = strings.Fields(p.MachineTags)
    p.DatesInfos.Upload, _ = timestampToSqlDate(p.DatesInfos.Upload)
    
    j, err := json.Marshal(p)

    if err != nil {
        return "", errors.New(fmt.Sprintf("%s : error while jsonifying %s", id, p))
    }

    return string(j), err
} 


/**
 * @brief retrieves details about an image from Flickr
 * @details [long description]
 * 
 * @param string owner id (nsid) of the owner
 * @param string date taken, in sql.date format
 * @return an array of Photo that have been taken by the owner at the given time
 * or an error
 */
func photoSearch(owner string, dateTaken string) ([]Photo, error) {

    dateTaken = url.QueryEscape(dateTaken)
    q := fmt.Sprintf("%s&user_id=%s&min_taken_date=%s&max_taken_date=%s&extras=%s", 
        SEARCH_URL, owner, dateTaken, dateTaken, url.QueryEscape(SEARCH_EXTRAS))
    //fmt.Println(q)
    resp, err := http.Get(q)
    if err != nil {
        panic(err)
    }

    rawBody, _ := ioutil.ReadAll(resp.Body)
    resp.Body.Close()

    var r Response
    err = xml.Unmarshal(rawBody, &r)
    CheckErr(err)


    return r.Photos, nil

}
