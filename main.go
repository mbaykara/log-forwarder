package main

import (
    "fmt"
    "net/http"
	  "io/ioutil"
	  gojsonq "github.com/thedevsaddam/gojsonq/v2"
)

func headers(w http.ResponseWriter, req *http.Request) {
		body, _ := ioutil.ReadAll(req.Body)
		jsonTOraw(body)
}

func jsonTOraw(log []byte){
	res := gojsonq.New().JSONString(string(log)).Find("log")
	str:=res.(string)
	if i := len(str)-1; str[i] == '"' {
        str = str[:i]
    }
	fmt.Println(str)
	
}
func main() {
	
		http.HandleFunc("/log", headers)
		http.ListenAndServe(":8090", nil)
}
