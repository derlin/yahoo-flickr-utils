package main

import (
    "fmt"
    "io"
    "net/http"
    "os"
    "strings"
    "bufio"
    "encoding/json"
)


type Photo struct {
    Id string `json:"id"`
    Url string `json:"url"`
}

func downloadFromUrl(url string) {
    tokens := strings.Split(url, "/")
    fileName := tokens[len(tokens)-1]
    //fmt.Println("Downloading", url, "to", fileName)

    // TODO: check file existence first with io.IsExist
    output, err := os.Create(fileName)
    if err != nil {
        fmt.Println("Error while creating", fileName, "-", err)
        return
    }
    defer output.Close()

    response, err := http.Get(url)
    if err != nil {
        fmt.Println("Error while downloading", url, "-", err)
        return
    }
    defer response.Body.Close()

    n, err := io.Copy(output, response.Body)
    if err != nil {
        fmt.Println("Error while downloading", url, "-", err)
        return
    }

    fmt.Println(n, "bytes downloaded.")
}


func ReadLineByLine(filepath string, out chan string) {
    defer close(out)
    // read file 
    file, err := os.Open(filepath)
    if err != nil {
        
        return 
    }

    defer file.Close()
    
    scanner := bufio.NewScanner(file)
    lines := 0 

    for scanner.Scan() {
        out <- scanner.Text()       
        lines++
    }

}


func workers(jid int, lines chan string, done chan bool){

    for running := true;; {
        var s string;

        s, running = <- lines;
        
        if !running {
            fmt.Printf("[%d] Sent done\n", jid)
            done <- true
            return
        }

        var p Photo
        json.Unmarshal([]byte(s), &p)

        if p.Url != "" {
            downloadFromUrl(p.Url)
        }else {
            fmt.Printf("%s : could not find url\n", p.Id)
        }
    }


}

func main() {

    argc := len(os.Args)
    if argc < 2 {
        fmt.Printf("usage: %s <json file>\n", os.Args[0])
        os.Exit(1)
    }


    c := make(chan string)
    done := make(chan bool)

    nbrProcesses := 3

    for i := 0; i < nbrProcesses; i++ {
        go workers(i,c,done)
    }

    go ReadLineByLine(os.Args[1], c)

    for i := 0; i < nbrProcesses; i++ {
        <- done
    }

    close(done)




}