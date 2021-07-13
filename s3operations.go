package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cheggaaa/pb/v3"
)

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
