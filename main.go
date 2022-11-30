package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/colinmarc/hdfs"
	"github.com/samber/lo"
	"net/url"
	"os"
	"strings"
)

func main() {
	var source string
	var destination string
	var control string

	flag.StringVar(&source, "s", "", "source HDFS URL")
	flag.StringVar(&destination, "d", "./", "local destination path")
	flag.StringVar(&control, "c", "./files.txt", "control file with a list of previously copied files")
	flag.Parse()

	if len(source) == 0 {
		fmt.Println("usage: replicator -s <source-url> -d <destination-dir> -c <control-file>")
		flag.PrintDefaults()
		os.Exit(-1)
	}

	parsedSource, err := url.Parse(source)
	if err != nil {
		fmt.Printf("error: %s", err.Error())
		os.Exit(1)
	}
	sourcePath := strings.TrimSuffix(parsedSource.Path, "/")

	parsedDestination, err := url.Parse(destination)
	if err != nil {
		fmt.Printf("error: %s", err.Error())
		os.Exit(2)
	}
	destinationPath := strings.TrimSuffix(parsedDestination.Path, "/")

	client, err := hdfs.New(parsedSource.Host)
	if err != nil {
		fmt.Printf("error: %s", err.Error())
		os.Exit(3)
	}

	files, err := client.ReadDir(parsedSource.Path)
	if err != nil {
		fmt.Printf("error: %s", err.Error())
		os.Exit(4)
	}

	loadedFiles, err := readLines(control)
	if err != nil {
		fmt.Printf("error: %s", err.Error())
		os.Exit(5)
	}

	loadFilesUpdated := false
	lo.ForEach[os.FileInfo](files, func(file os.FileInfo, _ int) {
		name := file.Name()
		if !file.IsDir() {
			exist := lo.Contains(loadedFiles, name)
			if !exist {
				fmt.Printf("copying file %s \n", name)
				err = client.CopyToLocal(
					fmt.Sprintf("%s/%s", sourcePath, name),
					fmt.Sprintf("%s/%s", destinationPath, name),
				)
				if err != nil {
					fmt.Printf("error: %s", err.Error())
					os.Exit(6)
				}
				loadedFiles = append(loadedFiles, name)
				loadFilesUpdated = true
			} else {
				fmt.Printf("skipping file %s \n", name)
			}
		}
	})

	if loadFilesUpdated {
		err = writeLines(loadedFiles, control)
		if err != nil {
			fmt.Printf("error: %s", err.Error())
			os.Exit(6)
		}
	}

}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return []string{}, nil
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

// writeLines writes the lines to the given file.
func writeLines(lines []string, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
	return w.Flush()
}
