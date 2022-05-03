package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
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
	ctx, cred := auth_spn()
	accountName := os.Getenv("STORAGE_ACCOUNT_NAME")
	if len(accountName) == 0 {
		log.Printf("Provide the STORAGE_ACCOUNT_NAME environement variable\n")
	}
	log.Printf("Container %s creating...\n", deployment)
	URL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	serviceClient, err := azblob.NewServiceClient(URL, cred, nil)
	if err != nil {
		log.Printf("Invalid credentials with while creating a servceClient error: %s" + err.Error())
	}

	containerClient, _ := serviceClient.NewContainerClient(deployment)
	_, err = containerClient.Create(ctx, nil)
	if err != nil {
		fmt.Printf("Error Code: %s", err)
	}
	log.Printf("Container %s created.\n", deployment)
	appendBlob(cred, accountName, deployment, k8scontainer, data, ctx)
	return azblob.ServiceClient{}

}

func appendBlob(cred *azidentity.DefaultAzureCredential, accountName, deployment, k8scontainer, data string, ctx context.Context) {
	blobname := time.Now().Format("20060102") + "-" + k8scontainer + ".log"
	u := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", accountName, deployment, blobname)
	appendBlobClient, err := azblob.NewAppendBlobClient(u, cred, nil)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("At line %d", 105)
	b := isBlobExist(cred, deployment, accountName, blobname, ctx)
	if !b {
		_, err = appendBlobClient.Create(ctx, nil)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Blob %s created or already exist", blobname)
	}
	data = data + "\n"
	r, err := appendBlobClient.AppendBlock(ctx, streaming.NopCloser(strings.NewReader(data)), nil)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Block appended to %s successfully.", blobname)
	UNUSED(r)
}

func isBlobExist(cred *azidentity.DefaultAzureCredential, container, accountName, blobname string, ctx context.Context) bool {
	u := fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, container)
	cclient, err := azblob.NewContainerClient(u, cred, nil)
	if err != nil {
		log.Fatal(err)
	}

	pager := cclient.ListBlobsFlat(nil)
	for pager.NextPage(ctx) {
		resp := pager.PageResponse()
		for _, v := range resp.ListBlobsFlatSegmentResponse.Segment.BlobItems {
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

func auth_spn() (context.Context, *azidentity.DefaultAzureCredential) {
	a, b, c, d := os.Getenv("AZURE_TENANT_ID"), os.Getenv("AZURE_CLIENT_ID"), os.Getenv("AZURE_CLIENT_SECRET"), os.Getenv("SUBSCRIPTION_ID")
	if len(a) == 0 || len(b) == 0 || len(c) == 0 || len(d) == 0 {
		fmt.Printf("Provide following env vars %s\n%s\n%s\n%s\n:", a, b, c, d)
	}
	ctx := context.Background()
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatalf("Authentication Failed %s ", err)
	}
	client, _ := armresources.NewClient(d, cred, nil)
	UNUSED(client)
	return ctx, cred
}
func UNUSED(x ...interface{}) {}
