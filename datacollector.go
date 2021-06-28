package main

import (
	config "datacollector/conf"
	"datacollector/log"
	"datacollector/mgr"
	"datacollector/times"
	"datacollector/utils"
	. "datacollector/utils/models"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	utilsos "datacollector/utils/os"

	"github.com/labstack/echo"
)

//Config of logAgent
type Config struct {
	MaxProcs          int      `json:"max_procs"`
	DebugLevel        int      `json:"debug_level"`
	ProfileHost       string   `json:"profile_host"`
	ConfsPath         []string `json:"confs_path"`
	LogPath           string   `json:"log"`
	CleanSelfLog      bool     `json:"clean_self_log"`
	CleanSelfDir      string   `json:"clean_self_dir"`
	CleanSelfPattern  string   `json:"clean_self_pattern"`
	CleanSelfDuration string   `json:"clean_self_duration"`
	CleanSelfLogCnt   int      `json:"clean_self_cnt"`
	TimeLayouts       []string `json:"timeformat_layouts"`
	StaticRootPath    string   `json:"static_root_path"`
	WorkerAddUrl      string   `json:"worker_add"`
	DeptUId           string   `json:"deptUId"`
	WorkerIp          string   `json:"workerIp"`
	InputReceive      string   `json:"input_receive"`
	JobStatus         string   `json:"jobs_status"`
	SqlCtLimit        float64  `json:"sql_ct_limit"`
	KeepAlive         string   `json:"keep_alive"`
	AppToken          string   `json:"appToken"`
	mgr.ManagerConfig
}

var (
	conf           Config
	workerIp       = "workerIp"
	workerHostName = "workerHostName"
	workType       = "workType"
	deptUId        = "deptUId"
	appToken       = "appToken"
)

const (
	NextVersion        = "v1.0"
	defaultReserveCnt  = 5
	defaultLogDir      = "./"
	defaultLogPattern  = "*.log-*"
	defaultLogDuration = 10 * time.Minute
	defaultRotateSize  = 100 * 1024 * 1024
	etlType            = "collect"
	PostMethod         = "POST"
)
const usage = `datacollector, Very easy-to-use server agent for collecting & sending data.
Usage:

datacollector [commands|flags]

The commands & flags are:

  -f <file>          configuration file to load

Examples:
  # start datacollector
  datacollector -f datacollector.conf
  `

var (
	confName = flag.String("f", "datacollector.conf", "configuration file to load")
)

func main() {
	flag.Usage = func() { usageExit(0) }
	flag.Parse()

	if err := config.LoadEx(&conf, *confName); err != nil {
		log.Fatal("config.Load failed:", err)
	}
	if conf.TimeLayouts != nil {
		times.AddLayout(conf.TimeLayouts)
	}
	if conf.MaxProcs <= 0 {
		conf.MaxProcs = NumCPU
	}
	MaxProcs = conf.MaxProcs
	runtime.GOMAXPROCS(conf.MaxProcs)
	log.SetOutputLevel(conf.DebugLevel)

	var (
		stopRotate         = make(chan struct{}, 0)
		logdir, logpattern string
		err                error
	)
	defer close(stopRotate)
	if conf.LogPath != "" {
		logdir, logpattern, err = utils.LogDirAndPattern(conf.LogPath)
		if err != nil {
			log.Fatal(err)
		}
		go loopRotateLogs(filepath.Join(logdir, logpattern), defaultRotateSize, 10*time.Second, stopRotate)
		conf.CleanSelfPattern = logpattern + "-*"
		conf.CleanSelfDir = logdir
		conf.ManagerConfig.CollectLogPath = filepath.Join(logdir, logpattern+"-*")
	}

	log.Infof("Welcome to use datacollector, Version: %v \n\nConfig: %#v", NextVersion, conf)
	m, err := mgr.NewManager(conf.ManagerConfig)
	if err != nil {
		log.Fatalf("NewManager: %v", err)
	}
	m.Version = NextVersion

	if m.CollectLogRunner != nil {
		go m.CollectLogRunner.Run()
		time.Sleep(time.Second) // 等待1秒让收集器启动
	}

	paths := getValidPath(conf.ConfsPath)
	if len(paths) <= 0 {
		log.Warnf("Cannot read or create any ConfsPath %v", conf.ConfsPath)
	}
	if err = m.Watch(paths); err != nil {
		log.Fatalf("watch path error %v", err)
	}
	m.RestoreWebDir()
	stopClean := make(chan struct{}, 0)
	defer close(stopClean)
	if conf.CleanSelfLog {
		if conf.CleanSelfDir == "" && logdir != "" {
			conf.CleanSelfDir = logdir
		}
		if conf.CleanSelfPattern == "" && logpattern != "" {
			conf.CleanSelfPattern = logpattern + "-*"
		}
		go loopCleanLogkitLog(conf.CleanSelfDir, conf.CleanSelfPattern, conf.CleanSelfLogCnt, conf.CleanSelfDuration, stopClean)
	}
	if len(conf.BindHost) > 0 {
		m.BindHost = conf.BindHost
	}
	e := echo.New()
	e.Static("/", conf.StaticRootPath)

	/*
	 *ztt add:register logAgent to pipeline
	 */
	if len(conf.WorkerAddUrl) > 0 {
		hostname := utilsos.GetOSInfo().Hostname
		var registerList = map[string]string{
			workerIp:       conf.WorkerIp,
			workerHostName: hostname,
			workType:       etlType,
			deptUId:        conf.DeptUId,
		}
		marshal, e := json.Marshal(registerList)
		if e != nil {
			log.Errorf("Error:", e)
		}
		regData := string(marshal)
		respBody := utils.HttpCallToken(PostMethod, conf.WorkerAddUrl, strings.NewReader(regData), conf.AppToken)
		log.Infof("Response Data:%v\n", string(respBody))
	}
	// start rest service
	rs := mgr.NewRestService(m, e)
	if conf.ProfileHost != "" {
		log.Infof("go profile_host was open at %v", conf.ProfileHost)
		go func() {
			log.Fatal(http.ListenAndServe(conf.ProfileHost, nil))
		}()
	}

	keepAliveUrl := conf.KeepAlive
	workerIp := conf.WorkerIp
	deptUId := conf.DeptUId
	go func(keepAliveUrl string, workerIp string, deptUId string) {
		for {
			keepAlive(keepAliveUrl, workerIp, deptUId, conf.AppToken)
			time.Sleep(time.Duration(1) * time.Minute)
		}
	}(keepAliveUrl, workerIp, deptUId)

	if err = rs.Register(); err != nil {
		log.Fatalf("register master error %v", err)
	}
	utilsos.WaitForInterrupt(func() {
		rs.Stop()
		if conf.CleanSelfLog {
			stopClean <- struct{}{}
		}
		m.Stop()
	})

}

type MatchFile struct {
	Name    string
	ModTime time.Time
}
type MatchFiles []MatchFile

func (f MatchFiles) Len() int           { return len(f) }
func (f MatchFiles) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
func (f MatchFiles) Less(i, j int) bool { return f[i].ModTime.Before(f[j].ModTime) }

func cleanLogkitLog(dir, pattern string, reserveCnt int) {
	var err error
	path := filepath.Join(dir, pattern)
	matches, err := filepath.Glob(path)
	if err != nil {
		log.Errorf("filepath.Glob path %v error %v", path, err)
		return
	}
	var files MatchFiles
	for _, name := range matches {
		info, err := os.Stat(name)
		if err != nil {
			log.Errorf("os.Stat name %v error %v", name, err)
			continue
		}
		files = append(files, MatchFile{
			Name:    name,
			ModTime: info.ModTime(),
		})
	}
	if len(files) <= reserveCnt {
		return
	}
	sort.Sort(files)
	for _, f := range files[0 : len(files)-reserveCnt] {
		err := os.Remove(f.Name)
		if err != nil {
			log.Errorf("Remove %s failed , error: %v", f, err)
			continue
		}
	}
	return
}
func loopCleanLogkitLog(dir, pattern string, reserveCnt int, duration string, exitchan chan struct{}) {
	if len(dir) <= 0 {
		dir = defaultLogDir
	}
	if len(pattern) <= 0 {
		pattern = defaultLogPattern
	}
	if reserveCnt <= 0 {
		reserveCnt = defaultReserveCnt
	}
	var (
		dur time.Duration
		err error
	)
	if duration != "" {
		dur, err = time.ParseDuration(duration)
		if err != nil {
			log.Warnf("clean self duration parse failed: %v, Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h'. Use default duration: 10m", err)
			dur = defaultLogDuration
		}
	} else {
		dur = defaultLogDuration
	}
	ticker := time.NewTicker(dur)
	defer ticker.Stop()
	for {
		select {
		case <-exitchan:
			return
		case <-ticker.C:
			cleanLogkitLog(dir, pattern, reserveCnt)
		}
	}
}
func rotateLog(path string) (file *os.File, err error) {
	newfile := path + "-" + time.Now().Format("0102030405")
	file, err = os.OpenFile(newfile, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		err = fmt.Errorf("rotateLog open newfile %v err %v", newfile, err)
		return
	}
	log.SetOutput(file)
	return
}
func loopRotateLogs(path string, rotateSize int64, dur time.Duration, exitchan chan struct{}) {
	file, err := rotateLog(path)
	if err != nil {
		log.Fatal(err)
	}
	ticker := time.NewTicker(dur)
	defer ticker.Stop()
	for {
		select {
		case <-exitchan:
			return
		case <-ticker.C:
			info, err := file.Stat()
			if err != nil {
				log.Warnf("stat log error %v", err)
			} else {
				if info.Size() >= rotateSize {
					newfile, err := rotateLog(path)
					if err != nil {
						log.Errorf("rotate log %v error %v, use old log to write logAgent log", path, err)
					} else {
						file.Close()
						file = newfile
					}
				}
			}

		}
	}
}

func usageExit(rc int) {
	fmt.Println(usage)
	os.Exit(rc)
}
func getValidPath(confPaths []string) (paths []string) {
	paths = make([]string, 0)
	exits := make(map[string]bool)
	for _, v := range confPaths {
		rp, err := filepath.Abs(v)
		if err != nil {
			log.Errorf("Get real path of ConfsPath %v error %v, ignore it", v, rp)
			continue
		}
		if _, ok := exits[rp]; ok {
			log.Errorf("ConfsPath %v duplicated, ignore", rp)
			continue
		}
		exits[rp] = true
		paths = append(paths, rp)
	}
	return
}
func keepAlive(url string, worker string, dept string, token string) {
	hostname := utilsos.GetOSInfo().Hostname
	var registerList = map[string]string{
		workerIp:       worker,
		workerHostName: hostname,
		workType:       etlType,
		deptUId:        dept,
	}
	marshal, e := json.Marshal(registerList)
	if e != nil {
		log.Errorf("Error:", e)
	}
	regData := string(marshal)
	respBody := utils.HttpCallToken(PostMethod, url, strings.NewReader(regData), token)
	log.Infof("keep alive response:%v\n", string(respBody))
}
