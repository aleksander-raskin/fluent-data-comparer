package main

import (
	"fmt"
	"strings"

	"github.com/manifoldco/promptui"
)

func getInput(prompt string) string {
	fmt.Printf("%v: ", prompt)
	result, err := reader.ReadString('\n')
	checkError(err)
	return strings.TrimSpace(result)
}

func getBucket(str string) string {
	prompt := promptui.Select{
		Label: str,
		Items: []string{"cs-rawdata", "cs-rawdata-staging", "cs-rawdata-dev"},
	}
	_, bucket, err := prompt.Run()
	checkError(err)
	return bucket
}

func getPath(bucket, str string) string {
	prompt := promptui.Select{
		Label: str,
		Items: []string{bucket + "/LR1", bucket + "/LR1-sbs", bucket + "/LR1-BOT", bucket + "/T1", bucket + "/LR1-k8s"},
	}
	_, path, err := prompt.Run()
	checkError(err)
	path = strings.TrimPrefix(path, bucket+"/")
	fmt.Println(path)
	return path
}
