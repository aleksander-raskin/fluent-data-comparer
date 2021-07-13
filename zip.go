package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/cheggaaa/pb/v3"
)

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
