package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
)

func initFolders(bucket, path, date, hour, projectId string) string {
	prefix := fmt.Sprintf("%v/%v/%v/projectId=%v/", path, date, hour, projectId)
	os.MkdirAll(bucket+"/"+prefix, 0700)
	return prefix
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
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
