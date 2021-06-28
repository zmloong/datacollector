package utils

import (
	"datacollector/log"
	"datacollector/utils/models"
	utilsos "datacollector/utils/os"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
)

const (
	DefaultDirPerm = 0755
)
const (
	Mb = 1024 * 1024
)

// 创建目录，并返回日志模式
func LogDirAndPattern(logpath string) (dir, pattern string, err error) {
	dir, err = filepath.Abs(filepath.Dir(logpath))
	if err != nil {
		if !os.IsNotExist(err) {
			err = fmt.Errorf("get datacollector log dir error %v", err)
			return
		}
	}
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, DefaultDirPerm); err != nil {
			err = fmt.Errorf("create datacollector log dir error %v", err)
			return
		}
	}
	pattern = filepath.Base(logpath)
	return
}
func HttpCallToken(method string, url string, data io.Reader, token string) []byte {
	var respBody []byte
	request, err := http.NewRequest(method, url, data)
	request.Header.Set("Content-Type", "application/json;charset=UTF-8")
	request.Header.Set("appToken", token)
	if err != nil {
		panic(err)
	}
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		log.Infof("Post Error:%v\n", err)
	} else {
		respBody, _ = ioutil.ReadAll(resp.Body)
	}
	return respBody
}

// IsExist checks whether a file or directory exists.
// It returns false when the file or directory does not exist.
func IsExist(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}
func IsDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

//获取指定目录下的所有文件和对应inode
func GetFiles(runnerName, dirPath string) (map[string]string, error) {
	dir, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	var (
		files    = make(map[string]string)
		inodeStr string
	)
	for _, fi := range dir {
		if fi.IsDir() {
			continue
		}

		filePath := filepath.Join(dirPath, fi.Name())
		inode, err := utilsos.GetIdentifyIDByPath(filePath)
		if err != nil {
			log.Errorf("Runner[%s]update expire map get file: %s inode failed: %v, ignore...", runnerName, filePath, err)
		} else {
			inodeStr = strconv.FormatUint(inode, 10)
		}

		files[filePath] = inodeStr
	}

	return files, nil
}
func UpdateExpireMap(runnerName string, fileMap map[string]string, expireMap map[string]int64) {
	if expireMap == nil {
		expireMap = make(map[string]int64)
	}
	for realPath, inode := range fileMap {
		f, errOpen := os.Open(realPath)
		if errOpen != nil {
			log.Errorf("Runner[%s] update expire map open file: %s offset failed: %v, ignore...", runnerName, realPath, errOpen)
			continue
		}

		offset, errSeek := f.Seek(0, io.SeekEnd)
		if errSeek != nil {
			log.Errorf("Runner[%s] update expire map get file: %s offset failed: %v, ignore...", runnerName, realPath, errSeek)
			f.Close()
			continue
		}
		expireMap[inode+"_"+realPath] = offset
		f.Close()
	}
}
func CheckNotExistFile(runnerName string, expireMap map[string]int64) {
	for inodePath := range expireMap {
		arr := strings.SplitN(inodePath, "_", 2)
		if len(arr) < 2 {
			log.Errorf("Runner[%s] expect inode_path, but got: %s", runnerName, inodePath)
			return
		}
		// 不存在时删除
		if !IsExist(arr[1]) {
			delete(expireMap, inodePath)
		}
	}
}
func BatchFullOrTimeout(runnerName string, stopped *int32, batchLen, batchSize int64, lastSend time.Time,
	maxBatchLen, maxBatchSize, maxBatchInterval int) bool {
	// 达到最大行数
	if maxBatchLen > 0 && int(batchLen) >= maxBatchLen {
		log.Debugf("Runner[%v] meet the max batch length %v", runnerName, maxBatchLen)
		return true
	}
	// 达到最大字节数
	if maxBatchSize > 0 && int(batchSize) >= maxBatchSize {
		log.Debugf("Runner[%v] meet the max batch size %v", runnerName, maxBatchSize)
		return true
	}
	// 超过最长的发送间隔
	if time.Now().Sub(lastSend).Seconds() >= float64(maxBatchInterval) {
		log.Debugf("Runner[%v] meet the max batch send interval %v", runnerName, maxBatchInterval)
		return true
	}
	// 如果任务已经停止
	if atomic.LoadInt32(stopped) > 0 {
		if !models.IsSelfRunner(runnerName) {
			log.Warnf("Runner[%v] meet the stopped signal", runnerName)
		} else {
			log.Debugf("Runner[%v] meet the stopped signal", runnerName)
		}
		return true
	}
	return false
}

var JSONTool = jsoniter.Config{UseNumber: true}.Froze()

func DeepCopyByJSON(dst, src interface{}) {
	confBytes, err := JSONTool.Marshal(src)
	if err != nil {
		log.Errorf("DeepCopyByJSON marshal error %v, use same pointer", err)
		dst = src
		return
	}
	if err = JSONTool.Unmarshal(confBytes, dst); err != nil {
		log.Errorf("DeepCopyByJSON unmarshal error %v, use same pointer", err)
		dst = src
		return
	}
}
func GetKeyOfNotEmptyValueInMap(m map[string]string) (key string, exist bool) {
	for k, v := range m {
		if v != "" {
			return k, true
		}
	}
	return "", false
}
