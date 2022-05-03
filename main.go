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
	Stream     string `json:"stream"`
	Logtag     string `json:"logtag"`
	App_time   string `json:"app_time"`
	Loglevel   string `json:"loglevel"`
	Class      string `json:"class"`
	Log        string `json:"log"`
	Kubernetes Kubernetes
}
type Kubernetes struct {
	Pod         string `json:"pod_name"`
	Namespace   string `json:"namespace_name"`
	App         string `json:"app"`
	Container   string `json:"container_name"`
	Labels      Labels
	Annotations Annotations
}
type Labels struct {
	App  string `json:"app"`
	Type string `json:"type"`
}

type Annotations struct {
	Parser string `json:"fluentbit.io/parser"`
}

func main() {

	http.HandleFunc("/log", headers)
	log.Printf("Waiting for logs ...")
	http.ListenAndServe(":8090", nil)
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
	var data = ""
	if len(c[0].Kubernetes.Annotations.Parser) > 0 {
		data = c[0].App_time + " " + c[0].Loglevel + " " + c[0].Class + " - " + c[0].Log
	} else {
		data = c[0].Log
	}
	addContainer(data, c[0].Kubernetes.Labels.App, c[0].Kubernetes.Container)

}

func addContainer(data, deployment, k8scontainer string) azblob.ServiceClient {
	cred, accountName, accountKey := auth()
	ctx := context.Background()
	if !isContainerExist(accountName, accountKey, deployment, ctx) {
		log.Printf("Container %s creating...\n", deployment)
		URL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
		serviceClient, err := azblob.NewServiceClientWithSharedKey(URL, cred, nil)
		if err != nil {
			log.Printf("Invalid credentials with while creating a servceClient error: %s" + err.Error())
		}

		containerClient := serviceClient.NewContainerClient(deployment)
		_, err = containerClient.Create(ctx, nil)
		if err != nil {
			fmt.Printf("Error Code: %s", err)
		}
		log.Printf("Container %s created.\n", deployment)
	}
	appendBlob(cred, accountName, deployment, k8scontainer, data, ctx)
	return azblob.ServiceClient{}

}

func isContainerExist(accountName, accountKey, blobcontainer string, ctx context.Context) bool {
	cred, err := containerService.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		fmt.Print(err)
	}
	p := containerService.NewPipeline(cred, containerService.PipelineOptions{})
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))
	serviceURL := containerService.NewServiceURL(*u, p)
	for marker := (containerService.Marker{}); marker.NotDone(); {
		listContainer, _ := serviceURL.ListContainersSegment(ctx, marker, containerService.ListContainersSegmentOptions{})
		for _, val := range listContainer.ContainerItems {
			if blobcontainer == val.Name {
				log.Printf("Container with name %s is already exist, skip container creation.\n", blobcontainer)
				return true
			}
			marker = listContainer.NextMarker //Paging
		}
		log.Printf("%s not exist", blobcontainer)
	}
	return false

}

func appendBlob(cred *azblob.SharedKeyCredential, accountName, deployment, k8scontainer, data string, ctx context.Context) {
	blobname := time.Now().Format("20060102") + "-" + k8scontainer + ".log"
	u := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", accountName, deployment, blobname)
	appendBlobClient, err := azblob.NewAppendBlobClientWithSharedKey(u, cred, nil)
	if err != nil {
		log.Fatal(err)
	}

	b := isBlobExist(cred, deployment, accountName, blobname, ctx)
	if !b {
		_, err = appendBlobClient.Create(ctx, nil)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Blob %s created.", blobname)
	}
	data = data + "\n"
	r, err := appendBlobClient.AppendBlock(ctx, streaming.NopCloser(strings.NewReader(data)), nil)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Block appended to %s successfully.", blobname)
	UNUSED(r)
}

func isBlobExist(cred *azblob.SharedKeyCredential, container, accountName, blobname string, ctx context.Context) bool {
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
	return cred, accountName, accountKey
}
func UNUSED(x ...interface{}) {}
