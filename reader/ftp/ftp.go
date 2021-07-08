package ftp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"datacollector/conf"
	"datacollector/log"
	"datacollector/reader"
	. "datacollector/reader/config"
	"datacollector/utils/models"

	"github.com/axgle/mahonia"
	"github.com/jlaffaye/ftp"
)

var (
	_ reader.DaemonReader = &FtpReader{}
	_ reader.Reader       = &FtpReader{}
)

func init() {
	reader.RegisterConstructor("FTP", NewFtpReader)
}

type FtpReader struct {
	meta             *reader.Meta
	Server           string
	User             string
	Password         string
	WorkingDirectory string
	Port             int32
	FlushLines       bool
	Codec            string
	FilterPattern    string
	readChan         chan string
	errChan          chan error
	status           int32
	sourceIp         string
	initErr          error
	initErrLock      sync.RWMutex
	mux              sync.Mutex
	Conn             *ftp.ServerConn
	stopChan         chan struct{}
	msgChan          chan message
	statInterval     time.Duration
	decoder          mahonia.Decoder
}

func NewFtpReader(meta *reader.Meta, conf conf.MapConf) (reader.Reader, error) {
	server, err := conf.GetString("server")
	FilterPattern, _ := conf.GetStringOr("filterPattern", " *.*")
	User, err := conf.GetString("user")
	statIntervalDur, _ := conf.GetStringOr(KeyStatInterval, "15s")
	statInterval, err := time.ParseDuration(statIntervalDur)

	Password, err := conf.GetString("password")
	if err != nil {
		return nil, err
	}
	Port, _ := conf.GetInt32Or("port", 21)
	WorkingDirectory, _ := conf.GetString("workingDirectory")
	var decoder mahonia.Decoder
	encoding, _ := conf.GetStringOr("codec", "")
	encoding = strings.ToUpper(encoding)
	if encoding != models.DefaultEncodingWay {
		decoder = mahonia.NewDecoder(encoding)
		if decoder == nil {
			log.Warnf("Encoding Way [%v] is not supported, will read as utf-8", encoding)
		}
	}
	bind := server + ":" + strconv.Itoa(int(Port))
	conn, err := ftp.Dial(bind, ftp.DialWithTimeout(5*time.Second))

	return &FtpReader{
		meta:             meta,
		Server:           server,
		readChan:         make(chan string, 2),
		User:             User,
		Password:         Password,
		WorkingDirectory: WorkingDirectory,
		Port:             Port,
		FilterPattern:    FilterPattern,
		Conn:             conn,
		mux:              sync.Mutex{},
		statInterval:     statInterval,
		status:           StatusInit,
		stopChan:         make(chan struct{}),
		msgChan:          make(chan message),
		errChan:          make(chan error),
		decoder:          decoder,
		Codec:            encoding,
	}, nil
}

type message struct {
	result      string
	logpath     string
	currentFile string
}

func (r *FtpReader) FetchInitError() error {
	r.initErrLock.RLock()
	defer r.initErrLock.RUnlock()
	return r.initErr
}

func (r *FtpReader) Source() string {
	return r.Server
}

func (r *FtpReader) ReadLine() (string, error) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case info := <-r.readChan:
		r.sourceIp = info
		return r.readString(info), nil
	case err := <-r.errChan:
		return "", err
	case <-timer.C:
	}

	return "", r.FetchInitError()
}

func (r *FtpReader) SyncMeta() {
}

func (r *FtpReader) Start() (err error) {
	if r.isStopping() || r.hasStopped() {
		return errors.New("reader is stopping or has stopped")
	} else if !atomic.CompareAndSwapInt32(&r.status, StatusInit, StatusRunning) {
		log.Warnf("Runner[%v] %q daemon has already started and is running", r.meta.RunnerName, r.Name())
		return nil
	}

	go func() {

		ticker := time.NewTicker(r.statInterval)

		defer ticker.Stop()
		defer func() {
			if rec := recover(); rec != nil {
				log.Errorf("runner[%v] Reader %q panic and was recovered from %v", r.meta.RunnerName, r.Name(), rec)
			}
		}()
		err = r.Conn.Login(r.User, r.Password)

		if err != nil {
			log.Errorf("runner[%v] login failed!", r.Name())
			_ = r.Close()
		}
		for {
			err := r.Conn.NoOp()
			if err != nil {
				log.Errorf("Runner [%v] ping failed![%v]", r.Name(), err)
				_ = r.Close()
				break
			}
			select {
			case <-r.stopChan:
				atomic.StoreInt32(&r.status, StatusStopped)
				log.Infof("Runner[%v] %q daemon has stopped from running", r.meta.RunnerName, r.Name())
				return
			case <-ticker.C:
			}
			r.discoverElements(*r.Conn, r.WorkingDirectory, "")
		}
	}()

	return
}

func (r *FtpReader) Close() error {
	if !atomic.CompareAndSwapInt32(&r.status, StatusRunning, StatusStopping) {
		log.Warnf("Runner[%v] reader %q is not running, close operation ignored", r.meta.RunnerName, r.Name())
		return nil
	}
	log.Debugf("Runner[%v] %q daemon is stopping", r.meta.RunnerName, r.Name())
	close(r.stopChan)
	return r.stop()
}

func (r *FtpReader) isStopping() bool {
	return atomic.LoadInt32(&r.status) == StatusStopping
}

func (r *FtpReader) hasStopped() bool {
	return atomic.LoadInt32(&r.status) == StatusStopped
}

func (r *FtpReader) Name() string {
	return "FtpReader<" + r.Server + ">"
}

func (*FtpReader) SetMode(_ string, _ interface{}) error {
	return errors.New("ftp reader does not support read mode")
}

func (r *FtpReader) sendError(err error) {
	if err == nil {
		return
	}
	defer func() {
		if rec := recover(); rec != nil {
			log.Errorf("runner[%v] Reader %q panic and was recovered from %v", r.meta.RunnerName, r.Name(), rec)
		}
	}()

	if atomic.LoadInt32(&r.status) == StatusStopped || atomic.LoadInt32(&r.status) == StatusStopping {
		return
	}
	r.errChan <- err
}

func (r *FtpReader) sendReadChan(value string) {
	buf := bytes.NewBuffer(nil)
	defer func() {
		if rec := recover(); rec != nil {
			log.Errorf("runner[%v] Reader %q panic and was recovered from %v", r.meta.RunnerName, r.Name(), rec)
		}
	}()
	if atomic.LoadInt32(&r.status) == StatusStopped || atomic.LoadInt32(&r.status) == StatusStopping {
		return
	}
	if strings.ToUpper(r.Codec) == "UNICODE" {
		value = unicodeToGBK(value, buf)
	}
	r.readChan <- value
}

func (r *FtpReader) stop() error {
	err := r.Conn.Quit()
	atomic.StoreInt32(&r.status, StatusStopped)
	return err
}

func WriteRecordsFile(fileInfo string, fileName string, metaName string) (err error) {
	var f *os.File
	/*joinMetaPath := path.Join("./meta/", runnerName, StatFileName)*/
	_, err = os.Stat(metaName)
	if err != nil {
		if os.IsNotExist(err) {
			_, err = os.Create(metaName)
			if err != nil {
				log.Errorf("Runner[%v] get dir failed: %v", fileName, err)
			}
		}
	}
	f, err = os.OpenFile(metaName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write([]byte(fileInfo))
	if err != nil {
		return err
	}
	return f.Sync()
}

func (r *FtpReader) IsExist(statFileName string, fileName string) bool {
	fileInfo := map[string]string{}
	f, err := os.Open(statFileName)
	if err != nil {
		log.Errorf("runner[%v] Reader meta file not exist!", r.Name())
	}
	defer f.Close()

	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}
		replaceLine := strings.Replace(line, "\n", "", -1)
		fileInfo[replaceLine] = replaceLine
	}
	if len(fileInfo[fileName]) > 0 {
		return true
	} else {
		return false
	}
}

func (r *FtpReader) discoverElements(conn ftp.ServerConn, parentDir string, currentDir string) {

	var list []string
	var dirToList = parentDir
	if len(currentDir) != 0 {
		dirToList = currentDir
	}
	nameList, err := conn.NameList(dirToList)
	if err != nil {
		log.Errorf("Runner[%v] get dir failed: %v", r.Name(), err)
	}
	list = nameList
	if len(list) > 0 {
		for _, elementName := range list {
			log.Printf("********************elementName:%s", elementName)
			isDir := IsDir(conn, elementName)
			if isDir {
				err := conn.ChangeDir(elementName)
				if err != nil {
					log.Errorf("Runner[%v] get dir failed: %v", r.Name(), err)
				}
				r.discoverElements(conn, dirToList, elementName)
			} else {
				log.Printf("********************文件名elementName:%s", elementName)
				matched, err := regexp.Match(r.FilterPattern, []byte(elementName))
				if err != nil {
					log.Errorf("matched failed!")
				}
				if matched {
					r.ReadData(conn, elementName)
				}
			}
		}
	}
}

func (r *FtpReader) ReadData(conn ftp.ServerConn, elementName string) {
	metaPath := r.meta.MetaFile()
	isExist := IsExist(metaPath, elementName)
	if !isExist {
		resp, err := conn.RetrFrom(elementName, 0)
		if err != nil {
			log.Errorf("FtpReader ReadData failed,can not connect ftp server!", err)
		}
		buf := bufio.NewReader(resp)
		for {
			bytes, err := buf.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					break
				}
			}
			r.sendReadChan(bytes)
		}
		_ = resp.Close()
		r.SyncMetaFtp(elementName)
	} else {
		return
	}
}

func IsDir(conn ftp.ServerConn, filename string) bool {
	err := conn.ChangeDir(filename)
	if err != nil {
		return false
	}
	return true
}

func IsExist(statFileName string, fileName string) bool {
	fileInfo := map[string]string{}
	f, err := os.Open(statFileName)
	if err != nil {
		log.Errorf("runner[%v] Reader meta file not exist!", statFileName)
	}
	defer f.Close()

	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}
		replaceLine := strings.Replace(line, "\n", "", -1)
		fileInfo[replaceLine] = replaceLine
	}
	if len(fileInfo[fileName]) > 0 {
		return true
	} else {
		return false
	}
}

func (r *FtpReader) SyncMetaFtp(fileName string) {
	r.mux.Lock()
	defer r.mux.Unlock()
	var fileInfo string
	fileInfo += fileName + "\n"
	err := WriteRecordsFile(fileInfo, fileName, r.meta.MetaFile())
	if err != nil {
		log.Errorf("Runner[%v] %v SyncMeta error %v", r.meta.RunnerName, fileName, err)
	}
}

func (r *FtpReader) readString(ret string) string {
	if r.decoder != nil {
		ret = r.decoder.ConvertString(ret)
	}
	return ret
}

func unicodeToGBK(str string, buf *bytes.Buffer) string {
	i, j := 0, len(str)
	for i < j {
		x := i + 6
		if x > j {
			buf.WriteString(str[i:])
			break
		}
		if str[i] == '\\' && str[i+1] == 'u' {
			hex := str[i+2 : x]
			r, err := strconv.ParseUint(hex, 16, 64)
			if err == nil {
				buf.WriteRune(rune(r))
			} else {
				buf.WriteString(str[i:x])
			}
			i = x
		} else {
			buf.WriteByte(str[i])
			i++
		}
	}

	return buf.String()
}
