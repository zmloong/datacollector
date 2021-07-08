package models

import (
	"datacollector/log"
	"datacollector/times"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	jsoniter "github.com/json-iterator/go"
)

type File struct {
	Info os.FileInfo
	Path string
}

// FileInfos attaches the methods of Interface to []int64, sorting in decreasing order.
type FileInfos []os.FileInfo

func (p FileInfos) Len() int           { return len(p) }
func (p FileInfos) Less(i, j int) bool { return ModTimeLater(p[i], p[j]) }
func (p FileInfos) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func Bool2String(i bool) string {
	if i {
		return "true"
	}
	return "false"
}
func ReadFileContent(path string) (content []string, err error) {
	body, err := ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Errorf("file %s not exisit", path)
			return
		}
		log.Errorf("read file %s error %v", path, err)
		return
	}
	content = TrimeList(strings.Split(string(body), "\n"))
	return
}
func TrimeList(strs []string) (ret []string) {
	for _, s := range strs {
		s = strings.TrimSpace(s)
		if len(s) <= 0 {
			continue
		}
		ret = append(ret, s)
	}
	return
}

// GetRealPath 处理软链接等，找到文件真实路径
func GetRealPath(path string) (newPath string, fi os.FileInfo, err error) {
	newPath = path
	fi, err = os.Lstat(path)
	if err != nil {
		return
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		newPath, err = filepath.EvalSymlinks(path)
		if err != nil {
			return
		}
		log.Infof("%s is symbol link to %v", path, newPath)
		fi, err = os.Lstat(newPath)
	}
	newPath, err = filepath.Abs(newPath)
	if err != nil {
		return
	}
	return
}

// ModTimeLater 按最后修改时间进行比较
func ModTimeLater(f1, f2 os.FileInfo) bool {
	if f1.ModTime().UnixNano() != f2.ModTime().UnixNano() {
		return f1.ModTime().UnixNano() > f2.ModTime().UnixNano()
	}
	return f1.Name() > f2.Name()
}

// ReadDirByTime 读取文件目录后按时间排序，时间最新的文件在前
func ReadDirByTime(dir string) (files []os.FileInfo, err error) {
	files, err = ioutil.ReadDir(dir)
	if err != nil {
		err = fmt.Errorf("ioutil.ReadDir(%s): %v, err:%v", dir, files, err)
		return
	}
	files = SortFilesByTime(files)
	return
}

// SortFilesByTime 按照文件更新的unixnano从大到小排，即最新的文件在前,相同时间的则按照文件名字典序，字典序在后面的排在前面
func SortFilesByTime(files FileInfos) (soredfiles []os.FileInfo) {
	files.Sort()
	return files
}

// Sort is a convenience method.
func (p FileInfos) Sort() { sort.Sort(p) }
func Hash(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	return strconv.Itoa(int(h.Sum32()))
}

//CreateDirIfNotExist 检查文件夹，不存在时创建
func CreateDirIfNotExist(dir string) (err error) {
	_, err = os.Stat(dir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(dir, os.ModeDir|os.ModePerm)
		if err != nil {
			return
		}
	}
	return
}

type KeyInfo struct {
	Valid  bool
	NewKey string
}

//注意：cache如果是nil，这个函数就完全没有意义，不如调用 DeepConvertKey
func DeepConvertKeyWithCache(data map[string]interface{}, cache map[string]KeyInfo) map[string]interface{} {
	for k, v := range data {
		if nv, ok := v.(map[string]interface{}); ok {
			v = DeepConvertKeyWithCache(nv, cache)
		} else if nv, ok := v.(Data); ok {
			v = DeepConvertKeyWithCache(nv, cache)
		}
		keyInfo, exist := cache[k]
		if !exist {
			keyInfo.NewKey, keyInfo.Valid = PandaKey(k)
			if cache == nil {
				cache = make(map[string]KeyInfo)
			}
			cache[k] = keyInfo
		}
		if !keyInfo.Valid {
			delete(data, k)
			data[keyInfo.NewKey] = v
		}
	}
	return data
}

// 判断时只有数字和字母为合法字符，规则：
// 1. 首字符为数字时，增加首字符 "K"
// 2. 首字符为非法字符时，去掉首字符（例如，如果字符串全为非法字符，则转换后为空）
// 3. 非首字符并且为非法字符时，使用 "_" 替代非法字符
// 返回key，以及原始key是否合法，如果不合法，则后续需要用转换后的key进行替换
func PandaKey(key string) (string, bool) {
	// check
	valid := true
	size := 0
	if key == "" {
		return "KEmptyPandoraAutoAdd", false
	}

	lastInderlineIndex := -1
	for idx, c := range key {
		if c >= '0' && c <= '9' {
			size++
			if idx == 0 {
				size++
				valid = false
			}
			continue
		}
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			size++
			continue
		}

		if c == '_' && idx == lastInderlineIndex+1 {
			valid = false
			lastInderlineIndex++
			continue
		}

		if idx > 0 && size > 0 {
			size++
		}
		valid = false
	}
	if valid {
		return key, true
	}

	if size <= 0 {
		return "", false
	}
	// set
	bytes := make([]byte, size)
	bp := 0
	lastBeReplace := false
	key = key[(lastInderlineIndex + 1):]
	for idx, c := range key {
		if c >= '0' && c <= '9' {
			if idx == 0 {
				bytes[bp] = 'K'
				bp++
			}
			bytes[bp] = byte(c)
			bp++
			lastBeReplace = false
			continue
		}
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			bytes[bp] = byte(c)
			bp++
			lastBeReplace = false
			continue
		}

		if c == '_' && bp > 0 {
			bytes[bp] = byte(c)
			bp++
			lastBeReplace = false
			continue
		}

		// 前一个字符被替换为 '_' 则当前字符不用 '_' 替换
		if bp > 0 && (!lastBeReplace) {
			bytes[bp] = '_'
			lastBeReplace = true
			bp++
		}
	}
	return string(bytes[:bp]), valid
}
func GetLogFiles(doneFilePath string) (files []File) {
	readDoneFileLines, err := ReadFileContent(doneFilePath)
	if err != nil {
		return
	}
	var readDoneFiles []string
	for _, v := range readDoneFileLines {
		sps := strings.Split(v, "\t")
		readDoneFiles = append(readDoneFiles, sps[0])
	}

	for i := len(readDoneFiles) - 1; i >= 0; i-- {
		df := readDoneFiles[i]
		dfi, err := os.Stat(df)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			log.Errorf("read file %v error %v", df, err)
			continue
		}
		files = append(files, File{
			Info: dfi,
			Path: df,
		})
	}
	return
}
func DecodeString(target string) (result string, err error) {
	if target != "" {
		bytes, err := base64.URLEncoding.DecodeString(target)
		if err != nil {
			err = fmt.Errorf("base64 decode %v error: %v", target, err)
			return "", err
		}
		result, err = url.PathUnescape(string(bytes))
		if err != nil {
			err = fmt.Errorf("path unescape decode %v error: %v", target, err)
			return "", err
		}
	}
	return
}
func GetGrokLabels(labelList []string, nameMap map[string]struct{}) (labels []GrokLabel) {
	labels = make([]GrokLabel, 0)
	for _, f := range labelList {
		parts := strings.Fields(f)
		if len(parts) < 2 {
			log.Errorf("label conf error: " + f + ", format should be \"labelName labelValue\", ignore this label...")
			continue
		}
		labelName, labelValue := parts[0], parts[1]
		if _, ok := nameMap[labelName]; ok {
			log.Errorf("label name %v was duplicated, ignore this lable <%v,%v>...", labelName, labelName, labelValue)
			continue
		}
		nameMap[labelName] = struct{}{}
		l := NewGrokLabel(labelName, labelValue)
		labels = append(labels, l)
	}
	return
}
func IsSelfRunner(runnerName string) bool {
	return strings.HasPrefix(runnerName, DefaultSelfRunnerName)
}
func IsSubmetaExpireValid(submetaExpire, expire time.Duration) bool {
	return submetaExpire.Nanoseconds() > 0 && submetaExpire < expire
}
func IsSubMetaExpire(submetaExpire, expire time.Duration) bool {
	return submetaExpire.Nanoseconds() > 0 && expire.Nanoseconds() > 0
}
func IsFileModified(path string, interval time.Duration, compare time.Time) bool {
	// time.NewTicker时不是严格的整数时间，例如 3s ,实际相差可能时3.0002s，此时如果在 3-3.5之间出现文件修改则检测不出来
	interval = interval + 500*time.Millisecond
	//如果周期设置的过短，这里的检查就要放大检查的时间，否则容易错过在短时间内真正有数据更新的文件，通常情况下至少一秒
	if interval < 3*time.Second {
		interval = 3 * time.Second
	}
	modTime := time.Now()
	fi, err := os.Stat(path)
	if err != nil {
		log.Warnf("Failed to get config modtime: %v", err)
	} else {
		modTime = fi.ModTime()
	}

	if modTime.Add(interval).Before(compare) {
		return false
	}

	return true
}

// MergeEnvTags 获取环境变量里的内容
func MergeEnvTags(name string, tags map[string]interface{}) map[string]interface{} {
	if name == "" {
		return tags
	}

	envTags := make(map[string]interface{})
	if value, exist := os.LookupEnv(name); exist {
		err := jsoniter.Unmarshal([]byte(value), &envTags)
		if err != nil {
			log.Warnf("get env tags unmarshal error: %v", err)
			return tags
		}
	} else {
		log.Warnf("env[%s] not exist", name)
	}

	if tags == nil {
		tags = make(map[string]interface{})
	}
	for k, v := range envTags {
		tags[k] = v
	}
	return tags
}
func AddTagsToData(tags map[string]interface{}, datas []Data, runnername string) []Data {
	for j, data := range datas {
		for k, v := range tags {
			if dt, ok := data[k]; ok {
				log.Debugf("Runner[%v] datasource tag already has data %v, ignore %v", runnername, dt, v)
			} else {
				data[k] = v
			}
		}
		datas[j] = data
	}
	return datas
}

//根据key字符串,拆分出层级keys数据
func GetKeys(keyStr string) []string {
	keys := strings.FieldsFunc(keyStr, isSeparator)
	return keys
}
func isSeparator(separator rune) bool {
	return separator == '.' || unicode.IsSpace(separator)
}

//通过层级key获取value.
//所有层级的map必须为 map[string]interface{} 类型.
//keys为空切片,返回原m
func GetMapValue(m map[string]interface{}, keys ...string) (interface{}, error) {
	var err error
	var val interface{}
	val = m
	curKeys := keys
	for i, k := range curKeys {
		//判断val是否为map[string]interface{}类型
		if m, ok := val.(Data); ok {
			val = map[string]interface{}(m)
		}
		if _, ok := val.(map[string]interface{}); ok {
			//判断val(k)是否存在
			if _, ok := val.(map[string]interface{})[k]; ok {
				val = val.(map[string]interface{})[k]
			} else {
				curKeys = curKeys[0 : i+1]
				err = fmt.Errorf("GetMapValue failed, keys %v are non-existent", keys)
				return nil, err
			}
		} else {
			err = fmt.Errorf("GetMapValue failed, %v is not the type of map[string]interface{}", val)
			return nil, err
		}
	}
	return val, err
}

//通过层级key删除key-val,并返回被删除的val,是否删除成功
//如果key不存在,则返回 nil,false
func DeleteMapValue(m map[string]interface{}, keys ...string) (interface{}, bool) {
	var val interface{}
	val = m
	for i, k := range keys {
		if m, ok := val.(Data); ok {
			val = map[string]interface{}(m)
		}
		if m, ok := val.(map[string]interface{}); ok {
			if temp, ok := m[k]; ok {
				if i == len(keys)-1 {
					delete(m, keys[len(keys)-1])
					return temp, true
				}
				val = temp
			} else {
				return nil, false
			}
		}
	}
	return nil, false
}

//通过层级key设置value值.
//如果key不存在,将会自动创建.
//当coercive为true时,会强制将非map[string]interface{}类型替换为map[string]interface{}类型,有可能导致数据丢失
func SetMapValue(m map[string]interface{}, val interface{}, coercive bool, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	curr := m
	for _, k := range keys[0 : len(keys)-1] {
		if _, ok := curr[k]; !ok {
			n := make(map[string]interface{})
			curr[k] = n
			curr = n
			continue
		}
		if _, ok := curr[k].(map[string]interface{}); !ok {
			if _, ok := curr[k].(Data); !ok {
				if coercive {
					n := make(map[string]interface{})
					curr[k] = n
				} else {
					err := fmt.Errorf("SetMapValue failed, %v is not the type of map[string]interface{}", curr[k])
					return err
				}
			}
		}
		if m, ok := curr[k].(Data); ok {
			curr = map[string]interface{}(m)
		} else {
			curr = curr[k].(map[string]interface{})
		}
	}
	curr[keys[len(keys)-1]] = val
	return nil
}
func ConvertDate(layoutBefore, layoutAfter string, offset int, loc *time.Location, v interface{}) (interface{}, error) {
	var s int64
	switch newv := v.(type) {
	case int64:
		s = newv
	case int:
		s = int64(newv)
	case int32:
		s = int64(newv)
	case int16:
		s = int64(newv)
	case uint64:
		s = int64(newv)
	case uint32:
		s = int64(newv)
	case string:
		newv = strings.Replace(newv, ",", ".", -1)
		if layoutBefore != "" {
			layoutBefore = strings.Replace(layoutBefore, ",", ".", -1)
			t, err := time.ParseInLocation(layoutBefore, newv, loc)
			if err != nil {
				return v, fmt.Errorf("can not parse %v with layout %v", newv, layoutBefore)
			}
			return FormatWithUserOption(layoutAfter, offset, t), nil
		}

		t, err := times.StrToTimeLocation(newv, loc)
		if err != nil {
			return v, err
		}
		return FormatWithUserOption(layoutAfter, offset, t), nil
	case json.Number:
		jsonNumber, err := newv.Int64()
		if err != nil {
			return v, err
		}
		s = jsonNumber
	case time.Time:
		return FormatWithUserOption(layoutAfter, offset, newv), nil
	case *time.Time:
		if newv == nil {
			return nil, nil
		}
		return FormatWithUserOption(layoutAfter, offset, *newv), nil
	default:
		return v, fmt.Errorf("can not parse %v type %v as date time", v, reflect.TypeOf(v))
	}
	news := s
	timestamp := strconv.FormatInt(news, 10)
	tm, err := GetTime(timestamp)
	if err != nil {
		return v, err
	}
	return FormatWithUserOption(layoutAfter, offset, tm), nil
}
func FormatWithUserOption(layoutAfter string, offset int, t time.Time) interface{} {
	t = t.Add(time.Duration(offset) * time.Hour)
	if t.Year() == 0 {
		t = t.AddDate(time.Now().Year(), 0, 0)
	}
	if layoutAfter != "" {
		return t.Format(layoutAfter)
	}

	return t.Format(time.RFC3339Nano)
}
func GetTime(timestamp string) (time.Time, error) {
	timeSecondPrecision := 19
	//补齐19位
	for i := len(timestamp); i < timeSecondPrecision; i++ {
		timestamp += "0"
	}
	// 取前19位，截取精度 纳妙
	timestamp = timestamp[0:timeSecondPrecision]
	t, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, t), nil
}
func TruncateStrSize(err string, size int) string {
	if len(err) <= size {
		return err
	}

	return fmt.Sprintf(err[:size]+"......(only show %d bytes, remain %d bytes)",
		size, len(err)-size)
}

type SchemaErr struct {
	Number int64
	Last   time.Time
}

func (s *SchemaErr) Output(count int64, err error) {
	s.Number += count
	if time.Now().Sub(s.Last) > 3*time.Second {
		log.Errorf("%v parse line errors occurred, same as %v", s.Number, err)
		s.Number = 0
		s.Last = time.Now()
	}
}
func RemoveHttpProtocal(url string) (hostport, schema string) {
	chttps := "https://"
	chttp := "http://"
	if strings.HasPrefix(url, chttp) {
		return strings.TrimPrefix(url, chttp), chttp
	}
	if strings.HasPrefix(url, chttps) {
		return strings.TrimPrefix(url, chttps), chttps
	}
	return url, chttp
}
func CheckErr(err error) error {
	se, ok := err.(*StatsError)
	var errorCnt int64
	if ok {
		if se.Errors == 0 && se.LastError == "" {
			return nil
		}
		errorCnt = se.Errors
		err = errors.New(se.LastError)
	} else {
		errorCnt = 1
	}

	if err != nil {
		return fmt.Errorf("%v parse line errors occurred, error %v", errorCnt, err.Error())
	}
	return nil
}
func ExtractField(slice []string) ([]string, error) {
	switch len(slice) {
	case 1:
		return slice, nil
	case 2:
		rgexpr := "^%\\{\\[\\S+\\]}$" // --->  %{[type]}
		r, _ := regexp.Compile(rgexpr)
		slice[0] = strings.TrimSpace(slice[0])
		bol := r.MatchString(slice[0])
		if bol {
			rs := []rune(slice[0])
			slice[0] = string(rs[3 : len(rs)-2])
			return slice, nil
		}
	default:
	}
	return nil, errors.New("parameters error,  you can write two parameters like: %%{[type]}, default or only one: default")
}
func ParseTimeZoneOffset(zoneoffset string) (ret int) {
	zoneoffset = strings.TrimSpace(zoneoffset)
	if zoneoffset == "" {
		return
	}
	mi := false
	if strings.HasPrefix(zoneoffset, "-") {
		mi = true
	}
	zoneoffset = strings.Trim(zoneoffset, "+-")
	i, err := strconv.ParseInt(zoneoffset, 10, 64)
	if err != nil {
		log.Errorf("parse %v error %v, ignore zoneoffset...", zoneoffset, err)
		return
	}
	ret = int(i)
	if mi {
		ret = 0 - ret
	}
	return
}
