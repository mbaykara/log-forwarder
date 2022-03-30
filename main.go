package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	gojsonq "github.com/thedevsaddam/gojsonq/v2"
)

func headers(w http.ResponseWriter, req *http.Request) {
		body, _ := ioutil.ReadAll(req.Body)
		jsonTOraw(body)
}

func jsonTOraw(log []byte){
	res := gojsonq.New().JSONString(string(log)).Find("log")
	fmt.Printf("rest %s",res)
	data := []byte(fmt.Sprint(res))
	var containername = "customcon3"
	cred,accountName:= auth()

	URL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	ctx := context.Background()


//disable if container exist////////////////////////////
	// serviceClient, err := azblob.NewServiceClientWithSharedKey(URL, cred, nil)
	// if err != nil {
	// 	fmt.Print("Invalid credentials with while creating a servceClient error: " + err.Error())
	// }

	// fmt.Printf("Creating a container named %s\n", containername)
	// containerClient := serviceClient.NewContainerClient(containername)
	// _, err = containerClient.Create(ctx, nil)
	// if err != nil {	fmt.Print("Error Code: %s",err)	}
///////////////////////////////////////////////////////////		

	fmt.Printf("\nUploading logs ...\n")
	
	blobName := "foo.txt"
	blobClient, err := azblob.NewBlockBlobClientWithSharedKey(URL+containername+"/"+blobName, cred, nil)
	if err != nil {
		fmt.Print(err)
	}
   fmt.Printf("Seccess ...\n")
	// Upload to data to blob storage
	_, err = blobClient.UploadBufferToBlockBlob(ctx, data, azblob.HighLevelUploadToBlockBlobOption{})
	if err != nil {
		fmt.Print("Failure to upload to blob: %+v", err)
	}
	fmt.Printf("Appending %s",containername)
	appendBlob(containername,blobName, data, ctx)

}

func main() {
	
		http.HandleFunc("/log", headers)
		http.ListenAndServe(":8090", nil)
}


func appendBlob(c ,b string, d []byte,ctx context.Context){
	cred, accountName := auth()
	u := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", accountName,c,b)
	appendBlobClient, err := azblob.NewAppendBlobClientWithSharedKey(u, cred, nil)
	if err != nil {
		log.Fatal(err)
	}
		_, err = appendBlobClient.Create(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	val := string(d)
	fmt.Println(val)
	_, err = appendBlobClient.AppendBlock(ctx, streaming.NopCloser(strings.NewReader(fmt.Sprintf("%s\n", val ))), nil)
		if err != nil {
			log.Fatal(err)
		}
}

func auth()(c *azblob.SharedKeyCredential, s string) {
	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT_NAME"), os.Getenv("AZURE_STORAGE_ACCOUNT_KEY")
	if len(accountName) == 0 || len(accountKey) == 0 {
		fmt.Print("Either the AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_ACCESS_KEY environment variable is not set")
	}
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}
	return cred,accountName
}