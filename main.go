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
	Time       int    `json:"date"`
	Stream     string `json:"stream"`
	Logtag     string `json:"logtag"`
	App_time   int    `json:"app_time"`
	Loglevel   string `json:"loglevel"`
	Class      string `json:"class"`
	Log        string `json:"log"`
	Kubernetes Kubernetes
}
type Kubernetes struct {
	Pod       string `json:"pod_name"`
	Namespace string `json:"namespace_name"`
	App       string `json:"app"`
	Labels    Labels
}
type Labels struct {
	App  string `json:"app"`
	Type string `json:"type"`
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
	addContainer(c[0].Log, c[0].Kubernetes.Labels.Type)

}

func addContainer(s, label string) azblob.ServiceClient {
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
	l := label
	appendBlob(cred, accountName, containerName, l, data, ctx)
	return azblob.ServiceClient{}

}

func main() {

	http.HandleFunc("/log", headers)
	log.Printf("Waiting for logs ...")
	http.ListenAndServe(":8090", nil)
}

func checkBlob(cred *azblob.SharedKeyCredential, container, accountName, blobname string, ctx context.Context) bool {
	u := fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, container)
	cclient, err := azblob.NewContainerClientWithSharedKey(u, cred, nil)
	if err != nil {
		log.Fatal(err)
	}
	pager := cclient.ListBlobsFlat(nil)

	for pager.NextPage(ctx) {
		resp := pager.PageResponse()

		for _, v := range resp.ContainerListBlobFlatSegmentResult.Segment.BlobItems {
			if *v.Name == blobname {
				return true
			}
		}
	}
	if err = pager.Err(); err != nil {
		log.Fatalf("Failure to list blobs: %+v", err)
	}
	return false
}
func appendBlob(cred *azblob.SharedKeyCredential, accountName, c, label string, d []byte, ctx context.Context) {
	blobname := time.Now().Format("02Jan2006") + label + ".txt"
	u := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", accountName, c, blobname)
	appendBlobClient, err := azblob.NewAppendBlobClientWithSharedKey(u, cred, nil)
	if err != nil {
		log.Fatal(err)
	}
	b := checkBlob(cred, c, accountName, blobname, ctx)

	if !b {
		_, err = appendBlobClient.Create(ctx, nil)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Blob %s created.", blobname)
	}
	val := string(d) + "\n"
	r, err := appendBlobClient.AppendBlock(ctx, streaming.NopCloser(strings.NewReader(val)), nil)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Block appended to %s successfully.", blobname)
	UNUSED(r)
}

func auth() (c *azblob.SharedKeyCredential, a, k string) {
	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT_NAME"), os.Getenv("AZURE_STORAGE_ACCOUNT_KEY")
	if len(accountName) == 0 || len(accountKey) == 0 {
		log.Printf("Either the AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_ACCOUNT_KEY environment variable is not set\n")
		os.Exit(123)
	}

	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Successfully authenticated.\n")
	return cred, accountName, accountKey
}
func UNUSED(x ...interface{}) {}
