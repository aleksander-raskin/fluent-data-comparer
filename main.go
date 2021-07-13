package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/manifoldco/promptui"
)

var reader = bufio.NewReader(os.Stdin)

func main() {

	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("eu-west-1")},
	)

	var bucket1 string
	flag.StringVar(&bucket1, "bucket1", "", "a date string")

	var path1 string
	flag.StringVar(&path1, "path1", "", "a date string")

	var bucket2 string
	flag.StringVar(&bucket2, "bucket2", "", "a date string")

	var path2 string
	flag.StringVar(&path2, "path2", "", "a date string")

	var date string
	flag.StringVar(&date, "date", "", "a date string")

	var hour string
	flag.StringVar(&hour, "hour", "", "a hour string")

	var projectId string
	flag.StringVar(&projectId, "projectId", "", "a projectId string")

	flag.Parse()

	if bucket1 == "" {
		bucket1 = getBucket("Select first bucket")
	}

	if path1 == "" {
		path1 = getPath(bucket1, "Select path")
	}

	if bucket2 == "" {
		bucket2 = getBucket("Select second bucket")
	}

	if path2 == "" {
		path2 = getPath(bucket2, "Select second path")
	}

	if date == "" {
		date = getInput("enter a date (20210712)")
	}

	if hour == "" {
		hour = getInput("enter a hour (00)")
	}

	if projectId == "" {
		projectId = getInput("enter a projectId (88)")
	}

	str := fmt.Sprintf("\n- %v\n- %v", bucket1+"/"+path1+"/"+date+"/"+hour+"/"+projectId, bucket2+"/"+path2+"/"+date+"/"+hour+"/"+projectId)
	prompt := promptui.Prompt{
		Label:     "compare",
		IsConfirm: true,
	}
	fmt.Println(str)
	result, err := prompt.Run()
	if err != nil {
		fmt.Printf("Declined %v\n", err)
		return
	}
	if result == "y" {
		routine(sess, bucket1, path1, date, hour, projectId)

		fmt.Println("")

		routine(sess, bucket2, path2, date, hour, projectId)

		fmt.Println("Finished")
	}

}

func routine(sess *session.Session, bucket, path, date, hour, projectId string) {
	prefix := initFolders(bucket, path, date, hour, projectId)
	downloadS3files(sess, bucket, listAllFilesInBucketPath(sess, bucket, prefix))

	folder := fmt.Sprintf("%v/%v/%v/%v/projectId=%v/", bucket, path, date, hour, projectId)
	unzipFilesInFolder(folder, true)
	fmt.Printf("%v has %v lines\n", folder, calculateLines(folder))
}
