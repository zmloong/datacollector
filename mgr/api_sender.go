package mgr

import (
	"net/http"
	"sort"

	"datacollector/router"
	. "datacollector/sender/config"
	. "datacollector/utils/models"

	"github.com/labstack/echo"
)

const KeySendConfig = "senders"
const KeyRouterConfig = "router"

// get /datacollector/sender/usages 获取sender用途说明
func (rs *RestService) GetSenderUsages() echo.HandlerFunc {
	return func(c echo.Context) error {
		sort.Stable(ModeUsages)
		return RespSuccess(c, ModeUsages)
	}
}

// get /datacollector/sender/options 获取sender配置参数
func (rs *RestService) GetSenderKeyOptions() echo.HandlerFunc {
	return func(c echo.Context) error {
		return RespSuccess(c, ModeKeyOptions)
	}
}

// get /datacollector/sender/router/option 获取所有sender router的配置项
func (rs *RestService) GetSenderRouterOption() echo.HandlerFunc {
	return func(c echo.Context) error {
		routerOption := router.GetRouterOption()
		return RespSuccess(c, routerOption)
	}
}

// get /datacollector/sender/router/usage 获取所有sender router匹配方式的名字和作用
func (rs *RestService) GetSenderRouterUsage() echo.HandlerFunc {
	return func(c echo.Context) error {
		routerUsage := router.GetRouterMatchTypeUsage()
		return RespSuccess(c, routerUsage)
	}
}

// POST /datacollector/sender/send 请求校验sender配置
func (rs *RestService) PostSend() echo.HandlerFunc {
	return func(c echo.Context) error {
		var senderConfig map[string]interface{} // request body params in map format
		if err := c.Bind(&senderConfig); err != nil {
			return RespError(c, http.StatusBadRequest, ErrSendSend, err.Error())
		}
		err := SendData(senderConfig)
		if err != nil {
			return RespError(c, http.StatusBadRequest, ErrSendSend, err.Error())
		}

		// Send Success
		return RespSuccess(c, nil)
	}
}

// POST /datacollector/sender/check 请求校验sender配置
func (rs *RestService) PostSenderCheck() echo.HandlerFunc {
	return func(c echo.Context) error {
		var senderConfig map[string]interface{} // request body params in map format
		if err := c.Bind(&senderConfig); err != nil {
			return RespError(c, http.StatusBadRequest, ErrSendSend, err.Error())
		}
		sendersConfig, err := getSendersConfig(senderConfig)
		if err != nil {
			return RespError(c, http.StatusBadRequest, ErrSendSend, err.Error())
		}
		_, err = getSenders(sendersConfig)
		if err != nil {
			return RespError(c, http.StatusBadRequest, ErrSendSend, err.Error())
		}

		// Check Success
		return RespSuccess(c, nil)
	}
}
