package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
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
	Path          string  `envconfig:"LOG_PATH" default:"/tmp/logs"`
	Interval      int     `envconfig:"INTERVAL" default:"600"`
	FileSizeLimit float64 `envconfig:"FILE_SIZE_LIMIT" default:"3"`
	LogLevel      string  `envconfig:"LOG_LEVEL" default:"INFO"`
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
	Parser    string `json:"fluentbit.io/parser"`
	SizeCheck bool
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
		uploadByInterval()
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

	var logItems []LogData

	// Limit request body size to 10MB to prevent memory exhaustion
	r.Body = http.MaxBytesReader(w, r.Body, 10*1024*1024)

	b, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Failed to read request body: %v", err)
		http.Error(w, "Request body too large or invalid", http.StatusBadRequest)
		return
	}
	if e.LogLevel == "DEBUG" {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	err = json.Unmarshal(b, &logItems)
	if err != nil {
		log.Printf("Failed to unmarshal JSON: %v", err)
		http.Error(w, "Invalid JSON format", http.StatusBadRequest)
		return
	}
	for i := range logItems {
		deployment := removeHash(logItems[i].Kubernetes.Pod)
		log.Debugf("Deployment name: %s\nPod name: ", deployment)
		log.Debugf("Pod name       : %s", logItems[i].Kubernetes.Pod)
		log.Debugf("Container name : %s", logItems[i].Kubernetes.Container)
		log.Debugf("The log message: %s", logItems[i].Message)
		log.Debugf("The log message: %s", logItems[i].Kubernetes.Annotations.SizeCheck)
		if strings.Contains(deployment, "backup") || strings.Contains(deployment, "setup") {
			break
		}
		if len(deployment) < 1 {
			break
		}

		writeBlob(logItems[i].Message, deployment, logItems[i].Kubernetes.Pod, logItems[i].Kubernetes.Annotations.SizeCheck)
	}

	// Send success response
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("OK")); err != nil {
		log.Errorf("Failed to write HTTP response: %v", err)
	}
}

func writeBlob(data, deployment, pod string, sizecheck bool) (string, string, string) {
	err := os.MkdirAll(e.Path, os.ModePerm)
	if err != nil {
		log.Errorf("Failed to create log directory: %v", err)
		return deployment, pod, ""
	}

	lfile := time.Now().Format("20060102") + "-" + pod + ".log"
	fullPath := filepath.Join(e.Path, lfile)

	f, err := os.OpenFile(fullPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("Cannot open the file: %v", err)
		return deployment, pod, lfile
	}
	defer f.Close()
	if _, err := f.WriteString(data + "\n"); err != nil {
		log.Errorf("Cannot write to the file: %v", err)
		return deployment, pod, lfile
	}
	fileSize := calculateSize(fullPath)
	if fileSize >= e.FileSizeLimit {
		uploadBySize(lfile)
	}
	return deployment, pod, lfile
}
func calculateSize(path string) float64 {
	file, err := os.Open(path)
	if err != nil {
		log.Errorf("Failed to open file for size calculation: %v", err)
		return 0
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		log.Errorf("Failed to stat file: %v", err)
		return 0
	}
	bytes := stat.Size()
	megabytes := float64(bytes) / (1024 * 1024)
	return megabytes
}

func addContainer() (context.Context, *azidentity.DefaultAzureCredential, string, string) {
	ctx, creds := authServicePrincipal()
	blobContainer := strings.ToLower(c.Cluster)
	u := fmt.Sprintf("https://%s.blob.core.windows.net/", c.Storageaccount)
	serviceClient, err := azblob.NewClient(u, creds, nil)
	if err != nil {
		log.Errorf("Invalid credentials while creating a serviceClient error: %s", err.Error())
	}
	_, err = serviceClient.CreateContainer(ctx, blobContainer, nil)
	if err != nil {
		log.Printf("Attempt to create container %s, but it exist. Skipping...", blobContainer)
	}
	return ctx, creds, blobContainer, c.Storageaccount
}

func uploadBySize(logfile string) {
	for _, lfile := range getLogFiles() {
		deploymentDir := prepareBlobName(lfile.Name())
		blobWithDir := deploymentDir + "/" + lfile.Name()
		fullPath := filepath.Join(e.Path, lfile.Name())
		b, err := os.ReadFile(fullPath)
		if err != nil {
			log.Errorf("No local blob file %s: %v", blobWithDir, err)
			continue
		}
		data := string(b)
		if lfile.Name() == logfile {
			uploadBlob(data, blobWithDir)
			err = os.Remove(fullPath)
			if err != nil {
				log.Errorf("Failed to remove file: %v", err)
			}
			break
		}
		log.Debugf("Data be appended: %s", data)
	}

	log.Println("The upload triggered by file size")

}

func uploadByInterval() {

	for _, lfile := range getLogFiles() {
		deploymentDir := prepareBlobName(lfile.Name())
		blobWithDir := deploymentDir + "/" + lfile.Name()
		fullPath := filepath.Join(e.Path, lfile.Name())
		b, err := os.ReadFile(fullPath)
		if err != nil {
			log.Errorf("No local blob file %s: %v", blobWithDir, err)
			continue
		}
		data := string(b)
		uploadBlob(data, blobWithDir)
		log.Debugf("Data be appended: %s", data)
	}
	err := os.RemoveAll(e.Path)
	if err != nil {
		log.Errorf("Failed to remove log directory: %v", err)
	}

	log.Println("The upload triggered by file interval")
}

func uploadBlob(data, blobWithDir string) {
	ctx, cred, blobContainer, accountName := addContainer()
	u := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", accountName, blobContainer, blobWithDir)
	appendBlobClient, err := appendblob.NewClient(u, cred, nil)
	if err != nil {
		log.Errorf("Failed to create appendBlobClient: %v", err)
		return
	}
	_, err = appendBlobClient.AppendBlock(ctx, streaming.NopCloser(strings.NewReader(data)), nil)
	if err != nil {
		_, err = appendBlobClient.Create(ctx, nil)
		if err != nil {
			log.Errorf("Failed to create new blob: %v", err)
			return
		}
		_, err = appendBlobClient.AppendBlock(ctx, streaming.NopCloser(strings.NewReader(data)), nil)
		if err != nil {
			log.Errorf("Failed to append the new Blob: %v", err)
			return
		}

	} else {
		log.Printf("Successfully appended to existing blob %s", blobWithDir)
	}

}

func getLogFiles() []fs.DirEntry {
	path := e.Path
	files, err := os.ReadDir(path)
	if err != nil {
		log.Errorf("Failed to read log directory: %v", err)
		return []fs.DirEntry{}
	}
	return files
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
