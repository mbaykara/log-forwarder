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
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
)

type Credentials struct {
	Client         string `envconfig:"AZURE_CLIENT_ID" required:"true"`
	Secret         string `envconfig:"AZURE_CLIENT_SECRET" required:"true"`
	Tenant         string `envconfig:"AZURE_TENANT_ID" required:"true"`
	Subs           string `envconfig:"SUBSCRIPTION_ID" required:"true"`
	Cluster        string `envconfig:"CLUSTER_NAME" required:"true"`
	Storageaccount string `envconfig:"STORAGE_ACCOUNT_NAME" required:"true"`
}

type EnvVars struct {
	Path     string `envconfig:"LOG_PATH" default:"/tmp/logs"`
	Interval int    `envconfig:"INTERVAL" default:"60"`
	loglevel string `envconfig:"LOG_LEVEL" default:"INFO"`
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

var (
	e EnvVars
	c Credentials
)

func main() {

	err := envconfig.Process("Interval", &e)
	if err != nil {
		log.Fatal(err.Error())
	}
	t := time.Duration(e.Interval)
	http.HandleFunc("/log", headers)
	ticker := time.NewTicker(t * time.Second)
	go schedule(ticker)
	log.Infof("Collecting logs under %s", e.Path)
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

	var c []LogData

	b, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
	}
	if e.loglevel == "DEBUG" {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	a := string(b)
	json.Unmarshal([]byte(a), &c)
	for i := range c {
		deployment := removeHash(c[i].Kubernetes.Pod)
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

func writeBlob(data, deployment, pod string) (string, string, string) {
	data = data + "\n"
	path := e.Path
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		log.Println(err)
	}
	err = os.Chdir(path)
	if err != nil {
		log.Warningf("Could not change to the deployment path %s", err)
	}
	lfile := time.Now().Format("20060102") + "-" + pod + ".log"

	f, err := os.OpenFile(lfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Cannot open the file %s", err)
	}
	defer f.Close()
	if _, err := f.WriteString(data + "\n"); err != nil {
		log.Fatalf("Cannot write to the file %s", err)
	}
	return deployment, pod, lfile
}

func addContainer() (context.Context, *azidentity.DefaultAzureCredential, string, string) {
	ctx, creds := authServicePrincipal()
	blobContainer := strings.ToLower(c.Cluster)
	u := fmt.Sprintf("https://%s.blob.core.windows.net/", c.Storageaccount)
	serviceClient, err := azblob.NewServiceClient(u, creds, nil)
	if err != nil {
		log.Errorf("Invalid credentials while creating a serviceClient error: %s", err.Error())
	}
	containerClient, _ := serviceClient.NewContainerClient(blobContainer)
	_, err = containerClient.Create(ctx, nil)
	if err != nil {
		log.Printf("Attempt to create container %s, but it exist. Skipping...", blobContainer)
	}
	return ctx, creds, blobContainer, c.Storageaccount
}
func uploadBlocks() {
	ctx, cred, blobContainer, accountName := addContainer()
	path := e.Path
	os.Chdir(path)
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}
	for _, lgile := range files {
		deploymentDir := prepareBlobName(lgile.Name())
		pod := lgile.Name()
		blobWithDir := deploymentDir + "/" + pod
		log.Debugf("Log file %s", pod)
		log.Debugf("Blob full path %s", blobWithDir)
		os.Chdir(deploymentDir)
		b, err := os.ReadFile(pod)
		if err != nil {
			fmt.Printf("No local blob file %s\n%s", err, blobWithDir)
		}
		data := string(b)
		log.Debugf("Data be appended: %s", data)
		u := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", accountName, blobContainer, blobWithDir)
		appendBlobClient, err := azblob.NewAppendBlobClient(u, cred, nil)
		if err != nil {
			log.Fatalf("Failed to create appendBlobClient %s", err)
		}
		_, err = appendBlobClient.AppendBlock(ctx, streaming.NopCloser(strings.NewReader(data)), nil)
		if err != nil {
			_, err = appendBlobClient.Create(ctx, nil)
			if err != nil {
				log.Printf("Failed to create new blob %s", err)
			}
			_, err = appendBlobClient.AppendBlock(ctx, streaming.NopCloser(strings.NewReader(data)), nil)
			if err != nil {
				log.Fatalf("Failed to append the new Blob %s", err)
			}

		} else {
			log.Printf("Successfully appended to existing blob %s", blobWithDir)
		}
	}
	err = os.RemoveAll(e.Path)
	if err != nil {
		log.Error(err)
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

	err := envconfig.Process("Client", &c)
	if err != nil {
		log.Fatal(err.Error())
	}
	return true
}
func UNUSED(x ...interface{}) {}
