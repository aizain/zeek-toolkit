// 36kr快讯

package news36kr

import (
	"encoding/json"
	"gorm.io/gorm"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	DefFlashReqPartnerId       = "web"
	DefFlashReqParamSiteId     = 1
	DefFlashReqParamPlatformId = 2
)

var flashUrl = "https://gateway.36kr.com/api/mis/nav/newsflash/flow"
var flashContentType = "application/json;charset=utf-8"

type FlashReqBody struct {
	PartnerId string            `json:"partner_id"`
	Timestamp int64             `json:"timestamp"`
	Param     FlashReqBodyParam `json:"param"`
}

type FlashReqBodyParam struct {
	PageSize     int    `json:"pageSize"`
	PageEvent    int    `json:"pageEvent"`
	PageCallback string `json:"pageCallback"`
	SiteId       int    `json:"siteId"`
	PlatformId   int    `json:"platformId"`
}

type FlashRespBody struct {
	Code int               `json:"code"`
	Data FlashRespBodyData `json:"data"`
}
type FlashRespBodyData struct {
	ItemList     []map[string]any `json:"itemList,omitempty"`
	PageCallback string           `json:"pageCallback,omitempty"`
	HasNextPage  int              `json:"hasNextPage,omitempty"`
}

type TableNewsflashes struct {
	gorm.Model
	TaskBatchId     string `gorm:"column:task_batch_id"`
	RespCode        int    `gorm:"column:resp_code"`
	RespItemIdx     int    `gorm:"column:resp_item_idx"`
	RespItemData    string `gorm:"column:resp_item_data"`
	ReqTimestamp    int64  `gorm:"column:req_timestamp"`
	ReqPageSize     int    `gorm:"column:req_page_size"`
	ReqPageEvent    int    `gorm:"column:req_page_event"`
	ReqPageCallback string `gorm:"column:req_page_callback"`
	ReqSiteId       int    `gorm:"column:req_site_id"`
	ReqPlatformId   int    `gorm:"column:req_platform_id"`
	ReqPartnerId    string `gorm:"column:req_partner_id"`
}

func (TableNewsflashes) TableName() string {
	return "newsflashes_v1"
}

func NewFlashReq(pageEvent int, pageCallback string) FlashReqBody {
	return FlashReqBody{
		PartnerId: DefFlashReqPartnerId,
		Timestamp: time.Now().UnixMilli(),
		Param: FlashReqBodyParam{
			PageSize:     20,
			PageEvent:    pageEvent,
			PageCallback: pageCallback,
			SiteId:       DefFlashReqParamSiteId,
			PlatformId:   DefFlashReqParamPlatformId,
		},
	}
}

func Crawl(reqBody FlashReqBody) (*FlashRespBody, error) {
	reqJson, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(
		flashUrl,
		flashContentType,
		strings.NewReader(string(reqJson)),
	)
	if err != nil {
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var respBody FlashRespBody
	err = json.Unmarshal(data, &respBody)
	if err != nil {
		return nil, err
	}

	return &respBody, nil
}
