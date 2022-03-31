package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	containerService "github.com/Azure/azure-storage-blob-go/azblob"
)

type CustomData struct {
	Time     int    `json:"date"`
	Stream   string `json:"stream"`
	Logtag   string `json:"logtag"`
	App_time int    `json:"app_time"`
	Loglevel string `json:"loglevel"`
	Class    string `json:"class"`
	Log      string `json:"log"`
}

func isContinerExist(accountName, accountKey, containerName string) bool {
	cred, err := containerService.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		fmt.Print(err)
	}
	p := containerService.NewPipeline(cred, containerService.PipelineOptions{})
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))
	serviceURL := containerService.NewServiceURL(*u, p)
	for marker := (containerService.Marker{}); marker.NotDone(); {
		listContainer, _ := serviceURL.ListContainersSegment(context.TODO(), marker, containerService.ListContainersSegmentOptions{})
		for _, val := range listContainer.ContainerItems {
			if containerName == val.Name {
				return true
			}
			marker = listContainer.NextMarker // Next Page
		}
	}
	return false

}

func headers(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
	}
	var c []CustomData
	json.Unmarshal([]byte(body), &c)
	if err != nil {
		log.Println(err)
	}
	fmt.Printf("Here is change the foo.log: %s\n", c[0].Log)
	addContainer(c[0].Log)
}

func addContainer(log string) {
	data := []byte(fmt.Sprint(log))
	fmt.Printf("Send data %s: \n", data)
	fmt.Printf("Send log %s: \n", data)
	var containerName = strings.ToLower("container" + time.Now().Format("02Jan2006"))

	cred, accountName, accountKey := auth()
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
		if err != nil {
			fmt.Printf("Error Code: %s", err)
		}
	}
	appendBlob(containerName, data)

}

func main() {

	http.HandleFunc("/log", headers)
	http.ListenAndServe(":8090", nil)
}

func appendBlob(c string, d []byte) {
	cred, accountName, accountKey := auth()
	fmt.Printf("%s", accountKey)
	blobname := time.Now().Format("02Jan2006-15") + ".txt"
	u := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", accountName, c, blobname)
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
	_, err = appendBlobClient.AppendBlock(context.TODO(), streaming.NopCloser(strings.NewReader(val)), nil)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Block appended successfully.")
}

func auth() (c *azblob.SharedKeyCredential, a, k string) {
	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT_NAME"), os.Getenv("AZURE_STORAGE_ACCOUNT_KEY")
	if len(accountName) == 0 || len(accountKey) == 0 {
		fmt.Print("Either the AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_ACCOUNT_KEY environment variable is not set")
	}
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}
	return cred, accountName, accountKey
}
