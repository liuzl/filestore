# filestore

An append only file store, files are splited by time and size. Time interval could be `day` or `hour`, `day` by default. 

## Usage

```go
package main

import (
	"log"

	"github.com/liuzl/filestore"
)

func main() {
	//fs, err := filestore.NewFileStore("db")
	fs, err := filestore.NewFileStorePro("db", "hour")
	if err != nil {
		log.Fatal(err)
	}
	data := []byte("hello world!\n")
	for i := 0; i < 100; i++ {
		if _, err = fs.Write(data); err != nil {
			log.Fatal(err)
		}
	}
	fs.Close()
}
```
