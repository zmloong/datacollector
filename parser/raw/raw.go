package raw

import (
	"strings"
	"time"

	"datacollector/conf"
	"datacollector/parser"
	. "datacollector/parser/config"
	. "datacollector/utils/models"
)

func init() {
	parser.RegisterConstructor(TypeRaw, NewParser)
}

type Parser struct {
	name                 string
	labels               []GrokLabel
	withTimeStamp        bool
	disableRecordErrData bool
	keyRaw               string
	keyTimestamp         string
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	labelList, _ := c.GetStringListOr(KeyLabels, []string{})
	withtimestamp, _ := c.GetBoolOr(KeyTimestamp, true)
	nameMap := make(map[string]struct{})
	labels := GetGrokLabels(labelList, nameMap)
	keyRaw := KeyRaw
	keyTimestamp := KeyTimestamp
	prefix, _ := c.GetStringOr(InternalKeyPrefix, "")
	if prefix != "" {
		prefix = strings.TrimSpace(prefix)
		keyRaw = prefix + keyRaw
		keyTimestamp = prefix + keyTimestamp
	}

	disableRecordErrData, _ := c.GetBoolOr(KeyDisableRecordErrData, false)

	return &Parser{
		name:                 name,
		labels:               labels,
		withTimeStamp:        withtimestamp,
		disableRecordErrData: disableRecordErrData,
		keyRaw:               keyRaw,
		keyTimestamp:         keyTimestamp,
	}, nil
}

func (p *Parser) Name() string {
	return p.name
}

func (p *Parser) Type() string {
	return TypeRaw
}

func (p *Parser) Parse(lines []string) ([]Data, error) {
	var (
		datas     = make([]Data, len(lines))
		se        = &StatsError{}
		dataIndex = 0
	)
	for idx, line := range lines {
		//raw格式的不应该trime空格，只需要判断剔除掉全空就好了
		if len(strings.TrimSpace(line)) <= 0 {
			se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, idx)
			continue
		}
		d := Data{}
		d[p.keyRaw] = line
		if p.withTimeStamp {
			d[p.keyTimestamp] = time.Now().UnixNano() / 1e6
			//d[p.keyTimestamp] = time.Now().Format(time.RFC3339Nano)
		}
		for _, label := range p.labels {
			d[label.Name] = label.Value
		}
		datas[dataIndex] = d
		dataIndex++
		se.AddSuccess()
	}

	datas = datas[:dataIndex]
	if se.Errors == 0 && len(se.DatasourceSkipIndex) == 0 {
		return datas, nil
	}
	return datas, se
}
