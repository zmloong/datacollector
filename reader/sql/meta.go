package sql

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"datacollector/log"
	"datacollector/reader"

	"datacollector/utils/models"
)

func RestoreMeta(meta *reader.Meta, rawSqls string, magicLagDur time.Duration) (offsets []int64, sqls []string, omitMeta bool) {
	now := time.Now().Add(-magicLagDur)
	sqls = UpdateSqls(rawSqls, now)
	omitMeta = true
	sqlAndOffsets, length, err := meta.ReadOffset()
	if err != nil {
		log.Errorf("Runner[%v] %v -meta data is corrupted err:%v, omit meta data", meta.RunnerName, meta.MetaFile(), err)
		return
	}
	tmps := strings.Split(sqlAndOffsets, SqlOffsetConnector)
	if int64(len(tmps)) != 2*length || int64(len(sqls)) != length {
		log.Errorf("Runner[%v] %v -meta file is not invalid sql meta file %v， omit meta data", meta.RunnerName, meta.MetaFile(), sqlAndOffsets)
		return
	}
	omitMeta = false
	offsets = make([]int64, length)
	for idx, sql := range sqls {
		syncSQL := strings.Replace(tmps[idx], "@", " ", -1)
		offset, err := strconv.ParseInt(tmps[idx+int(length)], 10, 64)
		if err != nil || sql != syncSQL {
			log.Errorf("Runner[%v] %v -meta file sql is out of date %v or parse offset err %v， omit this offset", meta.RunnerName, meta.MetaFile(), syncSQL, err)
		}
		offsets[idx] = offset
	}
	return
}

func RestoreMetaNew(meta *reader.Meta, rawSqls string, magicLagDur time.Duration) (offsets []int64, syncSQLs []string, omitMeta bool) {
	now := time.Now().Add(-magicLagDur)
	sqls := UpdateSqls(rawSqls, now)
	omitMeta = true
	sqlAndOffsets, length, _, err := meta.ReadOffsetForSql()
	if err != nil {
		log.Errorf("Runner[%v] %v -meta data is corrupted err:%v, omit meta data", meta.RunnerName, meta.MetaFile(), err)
		return
	}
	tmps := strings.Split(sqlAndOffsets, SqlOffsetConnector)
	if int64(len(tmps)) != 2*length || int64(len(sqls)) != length {
		log.Errorf("Runner[%v] %v -meta file is not invalid sql meta file %v， omit meta data", meta.RunnerName, meta.MetaFile(), sqlAndOffsets)
		return
	}
	omitMeta = false
	offsets = make([]int64, length)
	for idx, _ := range sqls {
		replace := strings.Replace(tmps[idx], "&&", " ", -1)
		syncSQLs = UpdateSqls(replace, now)
		offset, err := strconv.ParseInt(tmps[idx+int(length)], 10, 64)
		if err != nil {
			log.Errorf("Runner[%v] %v -meta file sql is out of date  or parse offset err %v， omit this offset", meta.RunnerName, meta.MetaFile(), err)
		}
		offsets[idx] = offset
	}
	return
}

func RestoreSqls(meta *reader.Meta) map[string]string {
	recordsDone, err := meta.ReadRecordsFile(DefaultDoneSqlsFile)
	if err != nil {
		log.Errorf("Runner[%v] %v -table done data is corrupted err:%v, omit table done data", meta.RunnerName, meta.DoneFilePath, err)
		return map[string]string{}
	}

	var result = make(map[string]string)
	for _, record := range recordsDone {
		tmpRecord := models.TrimeList(strings.Split(record, SqlOffsetConnector))
		if int64(len(tmpRecord)) != 2 {
			log.Errorf("Runner[%v] %v -meta Records done file is invalid sqls file %v， omit meta data", meta.RunnerName, meta.MetaFile(), record)
			continue
		}
		result[tmpRecord[0]] = tmpRecord[1]
	}
	return result
}

func RestoreTimestampIntOffset(doneFilePath string) (int64, map[string]string, error) {
	filename := fmt.Sprintf("%v.%v", reader.DoneFileName, TimestampRecordsFile)
	cacheMapFilename := fmt.Sprintf("%v.%v", reader.DoneFileName, CacheMapFile)

	filePath := filepath.Join(doneFilePath, filename)
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return 0, nil, err
	}
	tm, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return tm, nil, err
	}

	cacheMapFilePath := filepath.Join(doneFilePath, cacheMapFilename)
	data, err = ioutil.ReadFile(cacheMapFilePath)
	if err != nil {
		return tm, nil, err
	}
	cache := make(map[string]string)
	err = json.Unmarshal(data, &cache)
	if err != nil {
		return tm, nil, err
	}
	return tm, cache, nil
}

func RestoreTimestampOffset(doneFilePath string) (time.Time, map[string]string, error) {
	filename := fmt.Sprintf("%v.%v", reader.DoneFileName, TimestampRecordsFile)
	cacheMapFilename := fmt.Sprintf("%v.%v", reader.DoneFileName, CacheMapFile)

	filePath := filepath.Join(doneFilePath, filename)
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return time.Time{}, nil, err
	}

	tm, err := time.Parse(time.RFC3339Nano, string(data))
	if err != nil {
		return tm, nil, err
	}

	cacheMapFilePath := filepath.Join(doneFilePath, cacheMapFilename)
	data, err = ioutil.ReadFile(cacheMapFilePath)
	if err != nil {
		return tm, nil, err
	}
	cache := make(map[string]string)
	err = json.Unmarshal(data, &cache)
	if err != nil {
		return tm, nil, err
	}
	return tm, cache, nil
}

func RestoreTimestampStrOffset(doneFilePath string) (string, map[string]string, error) {
	filename := fmt.Sprintf("%v.%v", reader.DoneFileName, TimestampRecordsFile)
	cacheMapFilename := fmt.Sprintf("%v.%v", reader.DoneFileName, CacheMapFile)

	filePath := filepath.Join(doneFilePath, filename)
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", nil, err
	}

	tmStr := string(data)
	cacheMapFilePath := filepath.Join(doneFilePath, cacheMapFilename)
	data, err = ioutil.ReadFile(cacheMapFilePath)
	if err != nil {
		return "", nil, err
	}
	cache := make(map[string]string)
	err = json.Unmarshal(data, &cache)
	if err != nil {
		return "", nil, err
	}
	return tmStr, cache, nil
}

func WriteCacheMap(doneFilePath string, cache map[string]string) (err error) {
	var f *os.File
	filename := fmt.Sprintf("%v.%v", reader.DoneFileName, CacheMapFile)
	filePath := filepath.Join(doneFilePath, filename)
	// write to tmp file
	f, err = os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, models.DefaultFilePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := json.Marshal(cache)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err != nil {
		return err
	}

	return f.Sync()
}

func WriteTimestampOffset(doneFilePath, content string) (err error) {
	var f *os.File
	filename := fmt.Sprintf("%v.%v", reader.DoneFileName, TimestampRecordsFile)
	filePath := filepath.Join(doneFilePath, filename)
	// write to tmp file
	f, err = os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, models.DefaultFilePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write([]byte(content))
	if err != nil {
		return err
	}

	return f.Sync()
}

// WriteRecordsFile 将当前文件写入donefiel中
func WriteRecordsFile(doneFilePath, content string) (err error) {
	var f *os.File
	filename := fmt.Sprintf("%v.%v", reader.DoneFileName, DefaultDoneRecordsFile)
	filePath := filepath.Join(doneFilePath, filename)
	// write to tmp file
	f, err = os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, models.DefaultFilePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write([]byte(content))
	if err != nil {
		return err
	}

	return f.Sync()
}

// WriteSqlsFile 将当前文件写入donefiel中
func WriteSqlsFile(doneFilePath, content string) (err error) {
	var f *os.File
	filename := fmt.Sprintf("%v.%v", reader.DoneFileName, DefaultDoneSqlsFile)
	filePath := filepath.Join(doneFilePath, filename)
	// write to tmp file
	f, err = os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, models.DefaultFilePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write([]byte(content))
	if err != nil {
		return err
	}

	return f.Sync()
}
