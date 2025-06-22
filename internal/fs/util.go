package fs

import (
	"net/url"
	"path/filepath"
	"strings"
)

var (
	// internal used file extensions for initiation
	// these extensions are used to determine the file type for conversion
	// see: getConvertedFileTmp() for more details in intenal/io/chunk_writer.go
	supportedExtensionFormat    = []string{".csv", ".json", ".txt", ".tsv", ".xlsx"}
	supportedExtensionFormatMap = map[string]bool{}
)

func init() {
	// initialize supported extension formats
	for _, ext := range supportedExtensionFormat {
		supportedExtensionFormatMap[ext] = true
	}
}

func NearestCommonParentDir(filePaths []string) string {
	dir := filepath.Dir(filePaths[0])
	for _, filePath := range filePaths[1:] {
		parentDir := filepath.Dir(filePath)
		i := 0
		for ; i < len(strings.Split(dir, "/")) && strings.Split(dir, "/")[i] == strings.Split(parentDir, "/")[i]; i++ {
		}
		dir = strings.Join(strings.Split(dir, "/")[:i], "/")
	}
	return dir
}

func SplitExtension(path string) (string, string) {
	leftExt := ""
	rightExt := ""
	for {
		if filepath.Ext(path) == "" {
			break
		}
		if _, ok := supportedExtensionFormatMap[leftExt]; ok {
			// if leftExt is a supported extension, we stop here
			break
		}
		rightExt = leftExt + rightExt
		leftExt = filepath.Ext(path)
		path = path[:len(path)-len(leftExt)]
	}
	return leftExt, rightExt
}

func MaskedURI(uri string) string {
	u, _ := url.Parse(uri)
	return u.Scheme + "://" + filepath.Join(u.Host, u.Path)
}
