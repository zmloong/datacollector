package mgr

import (
	"datacollector/cleaner"

	"github.com/labstack/echo"
)

// get /datacollector/cleaner/options 获取解析选项
func (rs *RestService) GetCleanerKeyOptions() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, cleaner.ModeKeyOptions)
	}
}
