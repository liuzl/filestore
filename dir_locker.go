package filestore

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	lockFileName      = "file.lock"
	ErrInvalidDirName = errors.New("invalid dir name")
	ErrLockExists     = errors.New("dir lock already exists")
	ErrDirNotExists   = errors.New("dir does not exist")
)

type DirLocker struct {
	sync.RWMutex
	wg            sync.WaitGroup
	createDirChan chan string
	exitChan      chan uint32
	dirLockers    map[string]bool
}

func NewDirLocker() *DirLocker {
	dirLocker := &DirLocker{
		wg:            sync.WaitGroup{},
		createDirChan: make(chan string),
		exitChan:      make(chan uint32),
		dirLockers:    make(map[string]bool),
	}
	go dirLocker.createDirLoop()
	return dirLocker
}

func (self *DirLocker) createDirLoop() {
	self.wg.Add(1)
	defer self.wg.Done()
DONE:
	for {
		select {
		case <-self.exitChan:
			break DONE
		case dir := <-self.createDirChan:
			if err := createDir(dir); err != nil {
				continue
			}
		}
	}
}

func (self *DirLocker) Close() {
	close(self.exitChan)
	self.wg.Wait()
}

func createDir(dir string) error {
	_, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dir, 0700)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

func (self *DirLocker) getLockStatus(dir string) bool {
	self.RLock()
	defer self.RUnlock()
	if locked, ok := self.dirLockers[dir]; ok {
		return locked
	}
	return false
}

func (self *DirLocker) TryLockDir(dir string) error {
	if len(dir) == 0 {
		return ErrInvalidDirName
	}
	locked := self.getLockStatus(dir)
	if locked {
		return ErrLockExists
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		self.createDirChan <- dir
	}
	self.Lock()
	defer self.Unlock()
	for i := 0; i < 3; i++ {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			time.Sleep(time.Second)
			continue
		} else {
			if locked, ok := self.dirLockers[dir]; ok && locked {
				return ErrLockExists
			}
			lockPath := filepath.Join(dir, lockFileName)
			err := ioutil.WriteFile(lockPath, []byte(fmt.Sprintf("%d", os.Getpid())), 0755)
			if err != nil {
				return err
			}
			self.dirLockers[dir] = true
			return nil
		}
	}
	return ErrDirNotExists
}

func (self *DirLocker) UnlockDir(dir string) error {
	if len(dir) == 0 {
		return ErrInvalidDirName
	}
	self.Lock()
	defer self.Unlock()
	lockPath := filepath.Join(dir, lockFileName)
	err := os.Remove(lockPath)
	if err != nil {
		return err
	}
	self.dirLockers[dir] = false
	return nil
}
