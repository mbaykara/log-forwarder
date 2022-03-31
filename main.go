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

func checkContainer(accountName, accountKey, containerName string) bool {
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
				log.Printf("Container with name %s is already exist, skip container creation.\n", containerName)
				return true
			}
			marker = listContainer.NextMarker //Paging
		}
	}
	return false

}

func headers(w http.ResponseWriter, r *http.Request) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
	}
	var c []CustomData
	json.Unmarshal([]byte(b), &c)
	if err != nil {
		log.Println(err)
	}
	addContainer(c[0].Log)

}

func addContainer(s string) {
	cred, accountName, accountKey := auth()
	data := []byte(fmt.Sprint(s))
	var containerName = "fcclogs"
	ctx := context.Background()
	if !checkContainer(accountName, accountKey, containerName) {
		log.Printf("Container %s creating...\n", containerName)
		URL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
		serviceClient, err := azblob.NewServiceClientWithSharedKey(URL, cred, nil)
		if err != nil {
			fmt.Print("Invalid credentials with while creating a servceClient error: " + err.Error())
		}

		containerClient := serviceClient.NewContainerClient(containerName)
		_, err = containerClient.Create(ctx, nil)
		if err != nil {
			fmt.Printf("Error Code: %s", err)
		}
		log.Printf("Container %s created.\n", containerName)
	}
	appendBlob(containerName, data)

}

func main() {

	http.HandleFunc("/log", headers)
	http.ListenAndServe(":8090", nil)
}

func appendBlob(c string, d []byte) {
	cred, accountName, accountKey := auth()
	UNUSED(accountKey)
	blobname := time.Now().Format("02Jan2006-15") + ".txt"
	u := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", accountName, c, blobname)
	appendBlobClient, err := azblob.NewAppendBlobClientWithSharedKey(u, cred, nil)
	if err != nil {
		log.Fatal(err)
	}

	_, err = appendBlobClient.Create(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("AppendBlobClient created.")
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
		log.Printf("Either the AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_ACCOUNT_KEY environment variable is not set\n")
	}
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Successfully authenticated.\n")
	return cred, accountName, accountKey
}
func UNUSED(x ...interface{}) {}
