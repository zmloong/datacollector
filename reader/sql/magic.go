package sql

import (
	"fmt"
	"strings"
	"time"

	"datacollector/log"
)

// 仅支持 YYYY, YY, MM, DD, hh, mm, ss，不支持 M, D, h, m, s
func ConvertMagicIndex(magic string, now time.Time) (Ret string, index int) {
	switch magic {
	case "YYYY":
		return fmt.Sprintf("%d", now.Year()), YEAR
	case "YY":
		return fmt.Sprintf("%d", now.Year())[2:], YEAR
	case "MM":
		m := int(now.Month())
		return fmt.Sprintf("%02d", m), MONTH
	case "DD":
		d := int(now.Day())
		return fmt.Sprintf("%02d", d), DAY
	case "hh":
		h := now.Hour()
		return fmt.Sprintf("%02d", h), HOUR
	case "mm":
		m := now.Minute()
		return fmt.Sprintf("%02d", m), MINUTE
	case "ss":
		s := now.Second()
		return fmt.Sprintf("%02d", s), SECOND
	}
	return "", -1
}

type MagicRes struct {
	TimeStart   []int  // 按照 YY,MM,DD,hh,mm,ss 顺序记录时间位置
	TimeEnd     []int  // 按照 YY,MM,DD,hh,mm,ss 顺序记录时间长度
	RemainIndex []int  // 按顺序记录非时间字符串开始结束位置，去除 *
	Ret         string // 渲染结果，包含 *
}

// 渲染魔法变量
func GoMagicIndex(rawSql string, now time.Time) (MagicRes, error) {
	sps := strings.Split(rawSql, "@(") //@()，对于每个分片找右括号
	var magicRes = MagicRes{
		TimeStart:   []int{-1, -1, -1, -1, -1, -1},
		TimeEnd:     make([]int, 6),
		RemainIndex: []int{0},
		Ret:         sps[0],
	}
	recordIndex := len(magicRes.Ret)

	// 没有魔法变量的情况，例如 mytest*
	if len(sps) < 2 {
		magicRes.RemainIndex = append(magicRes.RemainIndex, RemoveWildcards(magicRes.Ret, recordIndex))
		return magicRes, nil
	}

	magicRes.RemainIndex = append(magicRes.RemainIndex, recordIndex)
	for idx := 1; idx < len(sps); idx++ {
		idxr := strings.Index(sps[idx], ")")
		if idxr == -1 {
			magicRes.Ret = rawSql
			return magicRes, nil
		}
		spsStr := sps[idx][0:idxr]
		if len(spsStr) < 2 {
			magicRes.Ret = rawSql
			return magicRes, fmt.Errorf(SupportReminder)
		}
		res, index := ConvertMagicIndex(sps[idx][0:idxr], now)
		if index == -1 {
			magicRes.Ret = rawSql
			return magicRes, fmt.Errorf(SupportReminder)
		}

		// 记录日期起始位置
		magicRes.TimeStart[index] = recordIndex
		magicRes.Ret += res
		recordIndex = len(magicRes.Ret)

		// 记录日期长度
		magicRes.TimeEnd[index] = recordIndex

		if idxr+1 < len(sps[idx]) {
			spsRemain := sps[idx][idxr+1:]
			magicRes.Ret += spsRemain
			if spsRemain == Wildcards {
				recordIndex = len(magicRes.Ret)
				continue
			}
			magicRes.RemainIndex = append(magicRes.RemainIndex, recordIndex)
			magicRes.RemainIndex = append(magicRes.RemainIndex, RemoveWildcards(spsRemain, len(magicRes.Ret)))
			recordIndex = len(magicRes.Ret)
		}
	}

	return magicRes, nil
}

// 若包含通配符，字段长度相应 - 1
func RemoveWildcards(checkWildcards string, length int) int {
	if strings.Contains(checkWildcards, Wildcards) {
		return length - 1
	}
	return length
}

func CheckMagic(rawSql string) (valid bool) {
	sps := strings.Split(rawSql, "@(") //@()，对于每个分片找右括号
	now := time.Now()

	for idx := 1; idx < len(sps); idx++ {
		idxr := strings.Index(sps[idx], ")")
		if idxr == -1 {
			return true
		}
		spsStr := sps[idx][0:idxr]
		if len(spsStr) < 2 {
			log.Errorf(SupportReminder)
			return false
		}

		_, index := ConvertMagicIndex(sps[idx][0:idxr], now)
		if index == -1 {
			log.Errorf(SupportReminder)
			return false
		}
	}

	return true
}
