package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
)

type Credentials struct {
	Client  string `envconfig:"AZURE_CLIENT_ID"`
	Secret  string `envconfig:"AZURE_CLIENT_SECRET"`
	Tenant  string `envconfig:"AZURE_TENANT_ID"`
	Subs    string `envconfig:"SUBSCRIPTION_ID"`
	Cluster string `envconfig:"CLUSTER_NAME"`
}

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

func addContainer(data, deployment, k8sContainerName string) azblob.ServiceClient {
	ctx, cred := authServicePrincipal()
	bl_con := strings.ToLower(os.Getenv("CLUSTER_NAME"))
	log.Printf("Validating existence of the container: %s", bl_con)
	accountName := os.Getenv("STORAGE_ACCOUNT_NAME")
	URL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	serviceClient, err := azblob.NewServiceClient(URL, cred, nil)
	if err != nil {
		log.Printf("Invalid credentials with while creating a servceClient error: %s" + err.Error())
	}
	log.Printf("Creating the container: %s", bl_con)
	containerClient, _ := serviceClient.NewContainerClient(bl_con)
	_, err = containerClient.Create(ctx, nil)
	if err != nil {
		log.Printf("Attempt to create container %s, but it exist.", bl_con)
	}

	appendBlob(cred, accountName, bl_con, deployment, k8sContainerName, data, ctx)
	return azblob.ServiceClient{}

}

func appendBlob(cred *azidentity.DefaultAzureCredential, accountName, bl_con, deployment, k8sContainerName, data string, ctx context.Context) {
	blobname := deployment + "/" + time.Now().Format("20060102") + "-" + k8sContainerName + ".log"
	blobnamefinal := time.Now().Format("20060102") + "-" + k8sContainerName + ".log"
	u := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", accountName, bl_con, blobname)
	appendBlobClient, err := azblob.NewAppendBlobClient(u, cred, nil)
	if err != nil {
		log.Fatal(err)
	}
	b := isBlobExist(cred, bl_con, accountName, blobnamefinal, ctx)
	if !b {
		_, err = appendBlobClient.Create(ctx, nil)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Blob %s already exist", blobname)
	}
	data = data + "\n"
	r, err := appendBlobClient.AppendBlock(ctx, streaming.NopCloser(strings.NewReader(data)), nil)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Block appended to %s successfully.", blobname)
	UNUSED(r)
}

func isBlobExist(cred *azidentity.DefaultAzureCredential, bl_con, accountName, blobname string, ctx context.Context) bool {
	u := fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, bl_con)
	cclient, err := azblob.NewContainerClient(u, cred, nil)
	if err != nil {
		log.Errorf("Failed to create container client %s", err)
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

func authServicePrincipal() (context.Context, *azidentity.DefaultAzureCredential) {
	if !authEnvVars() {
		log.Fatalln("Error: Authentication environment variables not found")
	}
	ctx := context.Background()
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatalf("Authentication Failed %s ", err)
	}
	return ctx, cred
}

func authEnvVars() bool {
	var c Credentials
	err := envconfig.Process("Client", &c)
	if err != nil {
		log.Fatal(err.Error())
	}
	return true
}
func UNUSED(x ...interface{}) {}
