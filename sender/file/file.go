package file

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/lestrrat-go/strftime"

	"datacollector/conf"
	"datacollector/log"
	"datacollector/sender"
	. "datacollector/sender/config"
	. "datacollector/utils/models"
)

var (
	_ sender.SkipDeepCopySender = &Sender{}
	_ sender.Sender             = &Sender{}
)

type Sender struct {
	name         string
	pattern      *strftime.Strftime
	timestampKey string
	marshalFunc  func([]Data) ([]byte, error)
	writers      *writerStore

	partition int
}

func init() {
	sender.RegisterConstructor(TypeFile, NewSender)
}

func newSender(name, pattern, timestampKey string, maxOpenFile int, marshalFunc func([]Data) ([]byte, error)) (*Sender, error) {
	p, err := strftime.New(pattern)
	if err != nil {
		return nil, err
	}

	// 如果没有指定 timestamp key 则表示同时只会写入一个文件，没必要维护更多的文件句柄
	if len(timestampKey) == 0 {
		maxOpenFile = 1
	}

	return &Sender{
		name:         name,
		pattern:      p,
		timestampKey: timestampKey,
		marshalFunc:  marshalFunc,
		writers:      newWriterStore(maxOpenFile),
	}, nil
}

// jsonMarshalWithNewLineFunc 将数据序列化为 JSON 并且在末尾追加换行符
func jsonMarshalWithNewLineFunc(datas []Data) ([]byte, error) {
	bytes, err := jsoniter.Marshal(datas)
	if err != nil {
		return nil, err
	}
	return append(bytes, '\n'), nil
}

func writeRawFunc(datas []Data) ([]byte, error) {
	var buf bytes.Buffer
	for _, d := range datas {
		raw := d["raw"]
		switch bts := raw.(type) {
		case string:
			buf.Write([]byte(bts))
			if !strings.HasSuffix(bts, "\n") {
				buf.Write([]byte("\n"))
			}
		case []byte:
			buf.Write(bts)
			if !strings.HasSuffix(string(bts), "\n") {
				buf.Write([]byte("\n"))
			}
		}
	}
	return buf.Bytes(), nil
}

func NewSender(conf conf.MapConf) (sender.Sender, error) {
	path, err := conf.GetString(KeyFileSenderPath)
	if err != nil {
		return nil, err
	}
	name, _ := conf.GetStringOr(KeyName, "fileSender:"+path)
	timestampKey, _ := conf.GetStringOr(KeyFileSenderTimestampKey, "")
	maxOpenFile, _ := conf.GetIntOr(KeyFileSenderMaxOpenFiles, defaultFileWriterPoolSize)
	rawMarshal, _ := conf.GetBoolOr(KeyFileWriteRaw, false)
	partition, _ := conf.GetIntOr(KeyFilePartition, 0)
	marshal := jsonMarshalWithNewLineFunc
	if rawMarshal {
		marshal = writeRawFunc
	}
	s, err := newSender(name, path, timestampKey, maxOpenFile, marshal)
	if err != nil {
		return nil, err
	}
	s.partition = partition
	return s, nil
}

func (s *Sender) Name() string {
	return s.name
}

func (*Sender) SkipDeepCopy() bool { return true }

func (s *Sender) Send(datas []Data) error {
	// 仅仅上报错误信息，但是日志会正常写出，所以不需要上层重试
	ste := &StatsError{
		Ft:         true,
		FtNotRetry: true,
	}
	nowStr := s.pattern.FormatString(time.Now())
	batchDatas := make(map[string][]Data, 1)

	// 如果没有设置 timestamp key 则直接赋值
	if len(s.timestampKey) == 0 {
		if s.partition <= 1 {
			batchDatas[nowStr] = datas
		} else {
			for i := 0; i < s.partition; i++ {
				batchDatas[getPartitionFolder(nowStr, i)] = make([]Data, 0)
			}
			for i, v := range datas {
				str := getPartitionFolder(nowStr, i%s.partition)
				batchDatas[str] = append(batchDatas[str], v)
			}
		}
	} else {
		var tStr string
		for i := range datas {
			key, ok := datas[i][s.timestampKey].(string)
			if ok {
				t, err := time.Parse(time.RFC3339Nano, key)
				if err != nil {
					ste.LastError = fmt.Sprintf("%s parse timestamp key %q failed: %v", s.Name(), key, err)
					t = time.Now()
				}
				tStr = s.pattern.FormatString(t)
			} else {
				tStr = nowStr
			}

			batchDatas[tStr] = append(batchDatas[tStr], datas[i])
		}
	}

	// 分批写入不同文件
	wg := &sync.WaitGroup{}
	for filename, datas := range batchDatas {
		wg.Add(1)
		go s.writeFile(filename, datas, wg)
	}
	wg.Wait()

	if ste.Errors > 0 {
		return ste
	}
	return nil
}

func (s *Sender) writeFile(filename string, datas []Data, wg *sync.WaitGroup) {
	defer wg.Done()
	datasBytes, err := s.marshalFunc(datas)
	if err != nil {
		log.Errorf("get datas bytes to file[%s] failed: %v, datas length: %d", filename, err, len(datas))
		return
	}
	_, err = s.writers.Write(filename, datasBytes)
	if err != nil {
		log.Errorf("write to file[%s] failed: %v, datas length: %d", filename, err, len(datas))
		return
	}
}

func (s *Sender) Close() error {
	return s.writers.Close()
}

func getPartitionFolder(nowStr string, idx int) string {
	base := filepath.Base(nowStr)
	dir := filepath.Dir(nowStr)
	return filepath.Join(dir, "partition"+strconv.Itoa(idx), base)
}
