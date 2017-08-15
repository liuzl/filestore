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
	fileDir      string
	fileMaxBytes int64
	writeFile    *os.File
	writeDate    string
	writeSeq     uint32
	writePos     int64
	wg           sync.WaitGroup
	exitChan     chan int32
	dirLocker    *DirLocker
	sync.Mutex
}

func NewFileStore(dir string) (*FileStore, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0700)
		if err != nil {
			return nil, err
		}
	}
	dirLocker := NewDirLocker()
	err := dirLocker.TryLockDir(dir)
	if err != nil {
		return nil, err
	}
	fs := &FileStore{
		fileDir:      dir,
		fileMaxBytes: maxDefault,
		exitChan:     make(chan int32),
		dirLocker:    dirLocker,
	}
	if err := fs.setup(); err != nil {
		return nil, err
	}
	go fs.ioLoop()
	return fs, nil
}

func (self *FileStore) Close() {
	self.Lock()
	defer self.Unlock()
	close(self.exitChan)
	self.wg.Wait()
	if self.writeFile != nil {
		self.writeFile.Sync()
		self.writeFile.Close()
	}
	self.dirLocker.UnlockDir(self.fileDir)
}

func (self *FileStore) setup() error {
	lastWriteDate, lastWriteSeq, err := self.resolveFileName(self.lastFileName())
	curDate := time.Now().Format("20060102")
	if err != nil || lastWriteDate < curDate {
		self.writeDate = curDate
		self.writeSeq = 0
		self.writePos = 0
		return nil
	}
	self.writeDate = curDate
	self.writeSeq = lastWriteSeq + 1
	self.writePos = 0
	return nil
}

func (self *FileStore) resolveFileName(name string) (string, uint32, error) {
	if len(name) != 18 {
		return "", 0, fmt.Errorf("invalid file name: ", name)
	}
	date := name[:8]
	seq, err := strconv.Atoi(name[8:14])
	if err != nil {
		return "", 0, fmt.Errorf("invalid file name: ", name)
	}
	return date, uint32(seq), nil
}

func (self *FileStore) lastFileName() string {
	name := ""
	files, err := ioutil.ReadDir(self.fileDir)
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

func (self *FileStore) ioLoop() {
	self.wg.Add(1)
	defer self.wg.Done()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
DONE:
	for {
		select {
		case <-ticker.C:
			self.Lock()
			if self.writeFile != nil {
				self.writeFile.Sync()
			}
			self.Unlock()
		case <-self.exitChan:
			break DONE
		}
	}
}

func (self *FileStore) Write(data []byte) error {
	self.Lock()
	defer self.Unlock()
	curDate := time.Now().Format("20060102")
	if self.writeDate < curDate {
		self.writeFile.Close()
		self.writeFile = nil
		self.writeDate = curDate
		self.writeSeq = 0
		self.writePos = 0
	} else if self.writePos > self.fileMaxBytes {
		self.writeFile.Close()
		self.writeFile = nil
		self.writeSeq += 1
		self.writePos = 0
	}
	if self.writeFile == nil {
		fileName := filepath.Join(self.fileDir,
			fmt.Sprintf("%s%06d.dat", self.writeDate, self.writeSeq))
		var err error
		self.writeFile, err = os.OpenFile(fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0600)
		if err != nil {
			return err
		}
	}
	writeBytes, err := self.writeFile.Write(data)
	if err != nil {
		return err
	}
	self.writePos += int64(writeBytes)
	return nil
}

func (self *FileStore) WriteLine(data []byte) error {
	return self.Write(append(data, '\n'))
}
