package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
)

type Credentials struct {
	Client  string `envconfig:"AZURE_CLIENT_ID" required:"true"`
	Secret  string `envconfig:"AZURE_CLIENT_SECRET" required:"true"`
	Tenant  string `envconfig:"AZURE_TENANT_ID" required:"true"`
	Subs    string `envconfig:"SUBSCRIPTION_ID" required:"true"`
	Cluster string `envconfig:"CLUSTER_NAME" required:"true"`
	Path    string `envconfig:"LOG_PATH" required:"true"`
}

type LogData struct {
	Stream     string `json:"stream"`
	Logtag     string `json:"logtag"`
	Message    string `json:"message"`
	Date       int    `json:"date"`
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

	interval, err := strconv.Atoi(os.Getenv("INTERVAL"))
	if err != nil {
		panic(err)
	}
	if !(len(os.Getenv("INTERVAL")) > 0) {
		interval = 900
	}
	t := time.Duration(interval)
	http.HandleFunc("/log", headers)
	ticker := time.NewTicker(t * time.Second)
	go schedule(ticker)
	http.ListenAndServe(":8090", nil)
}

func schedule(ticker *time.Ticker) {
	for {
		<-ticker.C
		uploadBlocks()
	}
}

func removeHash(s string) string {
	r := regexp.MustCompile(`-?([a-z1-9]{8,})?-[a-z1-9]{5}$`)
	return r.ReplaceAllString(s, "")
}

func prepareBlobName(s string) string {
	r := regexp.MustCompile(`-?([a-z1-9]{8,})?-[a-z1-9]{5}.log$`)
	tmp := r.ReplaceAllString(s, "")
	r2 := regexp.MustCompile(`^(\d){8}-`)
	return r2.ReplaceAllString(tmp, "")
}

func headers(w http.ResponseWriter, r *http.Request) {

	var (
		c          []LogData
		deployment string
	)
	b, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
	}
	if os.Getenv("LOG_LEVEL") == "DEBUG" {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	a := string(b)
	json.Unmarshal([]byte(a), &c)
	for i := range c {
		deployment = removeHash(c[i].Kubernetes.Pod)
		log.Debugf("Deployment name: %s", deployment)
		log.Debugf("Pod name       : %s", c[i].Kubernetes.Pod)
		log.Debugf("Container name : %s", c[i].Kubernetes.Container)
		log.Debugf("The log message: %s", c[i].Message)
		if strings.Contains(deployment, "backup") || strings.Contains(deployment, "setup") {
			break
		}
		if len(deployment) < 1 {
			break
		}

		writeBlob(c[i].Message, deployment, c[i].Kubernetes.Pod)
	}

}

func writeBlob(data, deployment, podName string) (string, string, string) {
	data = data + "\n"
	var dir string
	path := os.Getenv("LOG_PATH")
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		log.Println(err)
	}
	err = os.Chdir(path)
	if err != nil {
		log.Warningf("Could not change to the deployment path %s", err)
	}

	dir = time.Now().Format("20060102") + "-" + podName + ".log"

	f, err := os.OpenFile(dir, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Cannot open the file %s", err)
	}
	defer f.Close()
	if _, err := f.WriteString(data + "\n"); err != nil {
		log.Fatalf("Cannot write to the file %s", err)
	}
	log.Printf(" Collecting logs...It will be uploaded by %s secons interval", os.Getenv("INTERVAL"))

	return deployment, podName, dir
}

func addContainer() (context.Context, *azidentity.DefaultAzureCredential, string, string) {
	ctx, cred := authServicePrincipal()
	blobContainer := strings.ToLower(os.Getenv("CLUSTER_NAME"))
	accountName := os.Getenv("STORAGE_ACCOUNT_NAME")
	URL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	serviceClient, err := azblob.NewServiceClient(URL, cred, nil)
	if err != nil {
		log.Printf("Invalid credentials with while creating a serviceClient error: %s" + err.Error())
	}
	log.Printf("Creating the container: %s", blobContainer)
	containerClient, _ := serviceClient.NewContainerClient(blobContainer)
	_, err = containerClient.Create(ctx, nil)
	if err != nil {
		log.Printf("Attempt to create container %s, but it exist. Skipping...", blobContainer)
	}
	return ctx, cred, blobContainer, accountName
}
func uploadBlocks() {
	ctx, cred, blobContainer, accountName := addContainer()
	path := os.Getenv("LOG_PATH")
	os.Chdir(path)
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}
	for _, logfile := range files {
		deploymentName := prepareBlobName(logfile.Name())
		podName := logfile.Name()

		blobWithDir := deploymentName + "/" + podName
		UNUSED(deploymentName, podName, blobWithDir)
		log.Println(blobWithDir)
		f, err := os.Open(podName)
		if err != nil {
			log.Errorf("No such a file  ", err)
		}
		u := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", accountName, blobContainer, blobWithDir)
		blockblobClient, err := azblob.NewBlockBlobClient(u, cred, nil)
		if err != nil {
			log.Fatal(err)
		}
		_, err = blockblobClient.UploadFile(ctx, f, azblob.UploadOption{})
		if err != nil {
			log.Fatalf("Failure to upload to blob: %+v", err)
		}
		log.Infof("===> %s uploaded successfully.", blobWithDir)

	}
}

func authServicePrincipal() (context.Context, *azidentity.DefaultAzureCredential) {
	if !authEnvVars() {
		log.Fatalln("Error: Authentication environment variables not found")
	}
	ctx := context.Background()
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatalf("Authentication Failed %s", err)
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
