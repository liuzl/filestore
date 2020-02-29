package filestore

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

const (
	maxDefault = 1024 * 1024 * 1024 * 2
)

type FileStore struct {
	splitBy      string
	fileDir      string
	fileMaxBytes int64
	writeFile    *os.File
	writeDate    string
	writeSeq     uint32
	writePos     int64
	wg           sync.WaitGroup
	exitChan     chan int32
	sync.Mutex
}

func NewFileStore(dir string) (*FileStore, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}
	}
	fs := &FileStore{
		fileDir:      dir,
		fileMaxBytes: maxDefault,
		exitChan:     make(chan int32),
	}
	if err := fs.setup(); err != nil {
		return nil, err
	}
	go fs.ioLoop()
	return fs, nil
}

func NewFileStorePro(dir, splitBy string) (*FileStore, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}
	}
	fs := &FileStore{
		splitBy:      splitBy,
		fileDir:      dir,
		fileMaxBytes: maxDefault,
		exitChan:     make(chan int32),
	}
	if err := fs.setup(); err != nil {
		return nil, err
	}
	go fs.ioLoop()
	return fs, nil
}

func (f *FileStore) Close() {
	f.Lock()
	defer f.Unlock()
	close(f.exitChan)
	f.wg.Wait()
	if f.writeFile != nil {
		f.writeFile.Sync()
		f.writeFile.Close()
	}
}

func (f *FileStore) setup() error {
	lastWriteDate, lastWriteSeq, err := f.resolveFileName(f.lastFileName())
	now := time.Now()
	curDate := now.Format("20060102")
	if f.splitBy == "hour" {
		curDate = now.Format("2006010215")
	}
	if err != nil || lastWriteDate < curDate {
		f.writeDate = curDate
		f.writeSeq = 0
		f.writePos = 0
		return nil
	}
	f.writeDate = curDate
	f.writeSeq = lastWriteSeq + 1
	f.writePos = 0
	return nil
}

func (f *FileStore) resolveFileName(name string) (string, uint32, error) {
	if len(name) != 18 {
		return "", 0, fmt.Errorf("invalid file name: ", name)
	}
	var date string
	var seq int
	var err error
	if f.splitBy == "hour" {
		date = name[:10]
		seq, err = strconv.Atoi(name[10:14])
	} else {
		date = name[:8]
		seq, err = strconv.Atoi(name[8:14])
	}
	if err != nil {
		return "", 0, fmt.Errorf("invalid file name: ", name)
	}
	return date, uint32(seq), nil
}

func (f *FileStore) lastFileName() string {
	name := ""
	files, err := ioutil.ReadDir(f.fileDir)
	if err != nil {
		return ""
	}
	for _, f := range files {
		if !f.IsDir() {
			matched, err := regexp.Match(`^\d{14,14}.dat$`, []byte(f.Name()))
			if err != nil || !matched {
				continue
			}
			if name == "" || f.Name() > name {
				name = f.Name()
			}
		}
	}
	return name
}

func (f *FileStore) ioLoop() {
	f.wg.Add(1)
	defer f.wg.Done()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
DONE:
	for {
		select {
		case <-ticker.C:
			f.Lock()
			if f.writeFile != nil {
				f.writeFile.Sync()
			}
			f.Unlock()
		case <-f.exitChan:
			break DONE
		}
	}
}

func (f *FileStore) Write(data []byte) (int, error) {
	f.Lock()
	defer f.Unlock()
	now := time.Now()
	curDate := now.Format("20060102")
	if f.splitBy == "hour" {
		curDate = now.Format("2006010215")
	}
	if f.writeDate < curDate {
		f.writeFile.Close()
		f.writeFile = nil
		f.writeDate = curDate
		f.writeSeq = 0
		f.writePos = 0
	} else if f.writePos > f.fileMaxBytes {
		f.writeFile.Close()
		f.writeFile = nil
		f.writeSeq += 1
		f.writePos = 0
	}
	if f.writeFile == nil {
		var fileName string
		if f.splitBy == "hour" {
			fileName = filepath.Join(f.fileDir,
				fmt.Sprintf("%s%04d.dat", f.writeDate, f.writeSeq))
		} else {
			fileName = filepath.Join(f.fileDir,
				fmt.Sprintf("%s%06d.dat", f.writeDate, f.writeSeq))
		}
		var err error
		if f.writeFile, err = os.OpenFile(
			fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0600); err != nil {
			return 0, err
		}
	}
	writeBytes, err := f.writeFile.Write(data)
	if err != nil {
		return writeBytes, err
	}
	f.writePos += int64(writeBytes)
	return writeBytes, nil
}

func (f *FileStore) WriteLine(data []byte) (int, error) {
	return f.Write(append(data, '\n'))
}
