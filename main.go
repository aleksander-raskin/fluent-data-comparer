package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/cheggaaa/pb/v3"
)

// var reader = bufio.NewReader(os.Stdin)

func main() {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("eu-west-1")},
	)

	bucket := "cs-rawdata-dev" //getInput("bucket name")
	path := "LR1-sbs"          //getInput("first path (LR1)")
	date := "20210712"         //getInput("date (20210620)")
	hour := "09"               //getInput("hour (00)")
	projectId := "1602"        //getInput("projectId")

	routine(sess, bucket, path, date, hour, projectId)

	fmt.Println("")

	bucket = "cs-rawdata" //getInput("bucket name")
	path = "LR1"          //getInput("first path (LR1)")

	routine(sess, bucket, path, date, hour, projectId)

	fmt.Println("Finished")
}

func routine(sess *session.Session, bucket, path, date, hour, projectId string) {
	prefix := initFolders(bucket, path, date, hour, projectId)
	downloadS3files(sess, bucket, listAllFilesInBucketPath(sess, bucket, prefix))

	folder := fmt.Sprintf("%v/%v/%v/%v/projectId=%v/", bucket, path, date, hour, projectId)
	unzipFilesInFolder(folder, true)
	fmt.Printf("%v has %v lines\n", folder, calculateLines(folder))
}

// func getInput(prompt string) string {
// 	fmt.Printf("%v: ", prompt)
// 	result, _ := reader.ReadString('\n')
// 	return strings.TrimSpace(result)
// }

func initFolders(bucket, path, date, hour, projectId string) string {
	prefix := fmt.Sprintf("%v/%v/%v/projectId=%v/", path, date, hour, projectId)
	os.MkdirAll(bucket+"/"+prefix, 0700)
	return prefix
}

func listAllFilesInBucketPath(sess *session.Session, bucket, prefix string) []string {
	svc := s3.New(sess)
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
		// MaxKeys: aws.Int64(1),
	}

	response, err := svc.ListObjectsV2(input)
	checkError(err)

	var slice []string

	for _, v := range response.Contents {
		slice = append(slice, *v.Key)
	}

	fmt.Println("===")
	fmt.Printf("Found %v files in %v/%v\n", len(slice), bucket, prefix)
	return slice
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func downloadS3files(sess *session.Session, bucket string, files []string) {
	downloader := s3manager.NewDownloader(sess)
	fmt.Println("Downloading:")
	bar := pb.StartNew(len(files))
	bar.SetWriter(os.Stdout)
	for _, item := range files {
		file, err := os.Create(bucket + "/" + item)
		checkError(err)

		numBytes, err := downloader.Download(file,
			&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(item),
			})
		checkError(err)
		bar.Increment()
		fmt.Sprintf("%v", numBytes)
	}
	bar.Finish()
}

func unzipFilesInFolder(folder string, cleanup bool) {
	files := listFilesInFolder(folder)
	fmt.Println("Decompressing:")
	bar := pb.StartNew(len(files) - 1)
	bar.SetWriter(os.Stdout)
	for _, file := range files {
		if strings.Contains(file, ".gz") {
			gUnzipData(file)
			if cleanup {
				os.Remove(file)
			}
		}
		bar.Increment()
	}
	bar.Finish()
}

func listFilesInFolder(folder string) []string {
	var files []string

	err := filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
		files = append(files, path)
		return nil
	})
	checkError(err)

	return files
}

// https://gist.github.com/alex-ant/aeaaf497055590dacba760af24839b8d
func gUnzipData(file string) {
	data, err := ioutil.ReadFile(file)
	checkError(err)
	dataBytes := bytes.NewBuffer(data)

	var reader io.Reader
	reader, err = gzip.NewReader(dataBytes)
	checkError(err)

	var result bytes.Buffer
	_, err = result.ReadFrom(reader)
	checkError(err)

	err = ioutil.WriteFile(strings.TrimSuffix(file, ".gz"), result.Bytes(), 0644)
	checkError(err)
}

func calculateLineNumbers(fileName string) int {
	file, err := os.Open(fileName)
	checkError(err)

	scanner := bufio.NewScanner(file)

	lines := 0
	for scanner.Scan() {
		lines++
	}
	return lines
}

func calculateLines(folder string) int {
	files := listFilesInFolder(folder)
	lines := 0
	fmt.Println("Calculating:")
	for _, file := range files {
		lines += calculateLineNumbers(file)
	}
	return lines
}
