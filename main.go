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

type LogData struct {
	Stream     string `json:"stream"`
	Logtag     string `json:"logtag"`
	Message    string `json:"message"`
	Kubernetes Kubernetes
}
type Kubernetes struct {
	Pod         string `json:"pod_name"`
	Namespace   string `json:"namespace_name"`
	Container   string `json:"container_name"`
	Host        string `json:"host"`
	Image       string `json:"container_image"`
	Labels      Labels
	Annotations Annotations
}
type Labels struct {
	App      string `json:"app"`
	K8s_App  string `json:"k8s_app"`
	Type     string `json:"type"`
	Instance string `json:"app.kubernetes.io/instance"`
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
	var (
		c          []LogData
		data       string
		deployment string
	)
	b, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
	}

	json.Unmarshal([]byte(b), &c)
	if err != nil {
		log.Printf("Unmarshal error %s\n", err)
	}

	data = c[0].Message
	if os.Getenv("LOG_LEVEL") == "DEBUG" {
		log.Printf("Data to send blob storge : %s\n", data)
		log.Printf("Pod name : %s\n", c[0].Kubernetes.Pod)

		log.Printf("Deployment namespace : %s\n", c[0].Kubernetes.Namespace)
		log.Printf("Deployment Label : %s\n", c[0].Kubernetes.Labels.App)
		log.Printf("Deployment K8s App : %s\n", c[0].Kubernetes.Labels.K8s_App)
		log.Printf("Container name : %s\n", c[0].Kubernetes.Container)
		log.Printf("Container image : %s\n", c[0].Kubernetes.Image)
	}
	switch {
	case len(c[0].Kubernetes.Labels.App) > 0:
		deployment = c[0].Kubernetes.Labels.App
		addContainer(data, deployment, c[0].Kubernetes.Container)
	case len(c[0].Kubernetes.Labels.K8s_App) > 0:
		deployment = c[0].Kubernetes.Labels.K8s_App
		addContainer(data, deployment, c[0].Kubernetes.Container)
	default:
		deployment = removeHash(c[0].Kubernetes.Pod)
		if len(deployment) == 0 {
			deployment = c[0].Kubernetes.Container
		}
		addContainer(data, deployment, c[0].Kubernetes.Container)
	}

}

func removeHash(s string) string {
	var podName string
	for i := 0; i < len(s)-17; i++ {
		podName += s[i : i+1]
	}
	return podName
}
func addContainer(data, deployment, k8sContainerName string) azblob.ServiceClient {
	ctx, cred := authServicePrincipal()
	blobContainer := strings.ToLower(os.Getenv("CLUSTER_NAME"))
	log.Printf("Validating existence of the container: %s\n", blobContainer)
	accountName := os.Getenv("STORAGE_ACCOUNT_NAME")
	URL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	serviceClient, err := azblob.NewServiceClient(URL, cred, nil)
	if err != nil {
		log.Printf("Invalid credentials with while creating a serviceClient error: %s\n" + err.Error())
	}
	log.Printf("Creating the container: %s\n", blobContainer)
	containerClient, _ := serviceClient.NewContainerClient(blobContainer)
	_, err = containerClient.Create(ctx, nil)
	if err != nil {
		log.Printf("Attempt to create container %s, but it exist.\n", blobContainer)
	}

	appendBlob(cred, accountName, blobContainer, deployment, k8sContainerName, data, ctx)
	return azblob.ServiceClient{}

}

func appendBlob(cred *azidentity.DefaultAzureCredential, accountName, blobContainer, deployment, k8sContainerName, data string, ctx context.Context) {
	blobWithDir := deployment + "/" + time.Now().Format("20060102") + "-" + k8sContainerName + ".log"
	data = data + "\n"
	u := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", accountName, blobContainer, blobWithDir)
	appendBlobClient, err := azblob.NewAppendBlobClient(u, cred, nil)
	if err != nil {
		log.Fatalf("Failed to create appendBlobClient %s\n", err)
	}

	_, err = appendBlobClient.AppendBlock(ctx, streaming.NopCloser(strings.NewReader(data)), nil)
	if err != nil {
		log.Printf("Failed to append existing one %s", err)
		_, err = appendBlobClient.Create(ctx, nil)
		if err != nil {
			log.Printf("Failed to create new blob %s\n", err)
		}
		_, err = appendBlobClient.AppendBlock(ctx, streaming.NopCloser(strings.NewReader(data)), nil)
		if err != nil {
			log.Fatalf("Failed to create a new Blob %s", err)
		}
	}

	log.Printf("Block appended to %s successfully.\n", blobWithDir)

}

func authServicePrincipal() (context.Context, *azidentity.DefaultAzureCredential) {
	if !authEnvVars() {
		log.Fatalln("Error: Authentication environment variables not found")
	}
	ctx := context.Background()
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatalf("Authentication Failed %s\n", err)
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
