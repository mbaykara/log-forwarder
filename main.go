package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	containerService "github.com/Azure/azure-storage-blob-go/azblob"
	gojsonq "github.com/thedevsaddam/gojsonq/v2"
)

func headers(w http.ResponseWriter, req *http.Request) {
		body, _ := ioutil.ReadAll(req.Body)
		jsonTOraw(body)
}
func isContinerExist(accountName, accountKey, containerName string) bool {
	cred,err := containerService.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		fmt.Print(err)
	}
	p := containerService.NewPipeline(cred, containerService.PipelineOptions{})
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))
	serviceURL := containerService.NewServiceURL(*u, p)
	for marker := (containerService.Marker{}); marker.NotDone(); {
		listContainer, _ := serviceURL.ListContainersSegment(context.TODO(), marker,  containerService.ListContainersSegmentOptions{})
		for _, val := range listContainer.ContainerItems {
			if containerName == val.Name {
				return true
			}
			marker = listContainer.NextMarker // Next Page
		}
	}
	return false

}

func jsonTOraw(log []byte){
	res := gojsonq.New().JSONString(string(log)).Find("log")
	data := []byte(fmt.Sprint(res))
	var containerName = strings.ToLower("container"+time.Now().Format("02Jan2006"))
	
	cred,accountName,accountKey:= auth()
	ctx := context.Background()
	if !isContinerExist(accountName, accountKey, containerName) {
		URL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
		serviceClient, err := azblob.NewServiceClientWithSharedKey(URL, cred, nil)
		if err != nil {
			fmt.Print("Invalid credentials with while creating a servceClient error: " + err.Error())
		}

		fmt.Printf("Creating a container named %s\n", containerName)
		containerClient := serviceClient.NewContainerClient(containerName)
		_, err = containerClient.Create(ctx, nil)
		if err != nil {	fmt.Print("Error Code: %s",err)	}
	}

	fmt.Printf("Appending %s\n in following container: ",containerName)
	appendBlob(containerName, data)

}

func main() {
	
	http.HandleFunc("/log", headers)
	http.ListenAndServe(":8090", nil)
}


func appendBlob(c string, d []byte){
	cred, accountName,accountKey := auth()
	fmt.Print(accountKey)
	blobname := time.Now().Format("02Jan2006-150405")+".txt"
	u := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", accountName,c,blobname)
	log.Print("blob url created")
	appendBlobClient, err := azblob.NewAppendBlobClientWithSharedKey(u, cred, nil)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("appendBlobClient created")
		_, err = appendBlobClient.Create(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	val := string(d)
	fmt.Println(val)
	_, err = appendBlobClient.AppendBlock(context.TODO(), streaming.NopCloser(strings.NewReader(fmt.Sprintf("%s\n", val ))), nil)
		if err != nil {
			log.Fatal(err)
		}
	log.Print("AppendBlock processed")
   fmt.Print("\nDONE!")
}

func auth()(c *azblob.SharedKeyCredential, a, k string) {
	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT_NAME"), os.Getenv("AZURE_STORAGE_ACCOUNT_KEY")
	if len(accountName) == 0 || len(accountKey) == 0 {
		fmt.Print("Either the AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_ACCOUNT_KEY environment variable is not set")
	}
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}
	return cred,accountName,accountKey
}