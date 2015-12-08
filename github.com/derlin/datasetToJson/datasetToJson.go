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
)


const (
    SEARCH_URL = "https://api.flickr.com/services/rest/?method=flickr.photos.search&api_key=3b0156f4f9282d41826ad695c4082f61"
    SEARCH_EXTRAS = "description,tags,machine_tags,url_o"
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
    Title string    `xml:"photo,attr" json:"title"`
    Descr string    `xml:"description" json:"description"`
    Tags string     `xml:"tags,attr" json:"-"`
    TagsArray []string  `json:"tags"`
    MachineTags string       `xml:"machine_tags,attr" json:"-"`
    MachineTagsArray []string `json:"machine_tags"`
    Url string      `xml:"url_o,attr" json:"url"`
    Height int      `xml:"height_o,attr" json:"height"`
    Width int       `xml:"width_o,attr" json:"width"`
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
        
        if nbrProcesses < 0 || nbrProcesses > 10 {
            ExitErr("error: nbrProcesses must be between 1 and 10")
        }

    }
    
    dispatcher(os.Args[1], nbrProcesses)
}

// ------------------------------------------------

func dispatcher(filepath string, nbrProcesses int){

    in := make(chan string)
    outOk := make(chan *JsonResult)
    outError := make(chan error)


    // launch processes
    for i := 0; i < nbrProcesses; i++ {
        go process(i, in, outOk, outError)
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
        log.Println("Master: dispatched 1 line")
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


func process(jid int, in chan string, outOk chan *JsonResult, outError chan error) {

    log.Printf("Job %d starting\n", jid)

    for {

        line, ok := <- in
        if !ok {
            log.Printf("Job %d finished\n", jid)
            return
        }

        log.Printf("Job %d processing one line\n", jid)
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

        log.Printf("Job %d processed one line\n", jid)
    }

}

// ------------------------------------------------

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
    
    j, err := json.Marshal(p)

    if err != nil {
        return "", errors.New(fmt.Sprintf("%s : error while jsonifying %s", id, p))
    }

    return string(j), err
} 


func photoSearch(owner string, dateTaken string) ([]Photo, error) {

    dateTaken = url.QueryEscape(dateTaken)
    q := fmt.Sprintf("%s&user_id=%s&min_taken_date=%s&max_taken_date=%s&extras=%s", 
        SEARCH_URL, owner, dateTaken, dateTaken, url.QueryEscape(SEARCH_EXTRAS))
    // fmt.Println(q)
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
