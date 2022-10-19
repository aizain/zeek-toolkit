package main

import (
	"encoding/json"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"os"
	"time"
	"zeek/toolkit/spider/news36kr"
)

func main() {
	batchId := time.Now().Format("200601021504")

	// 初始化数据库连接
	db, err := InitDB("crawl_36kr")
	if err != nil {
		log.Fatalf("init crawl_36kr db has err: %s\n", err)
		return
	}

	// 采集36kr快讯入库
	news36krReqBody := news36kr.NewFlashReq(0, "")
	body, err := news36kr.Crawl(news36krReqBody)
	if err != nil {
		log.Fatalf("crawl news36kr has err: %s\n", err)
		return
	}

	for i, item := range body.Data.ItemList {
		itemData, _ := json.Marshal(item)
		db.Create(&news36kr.TableNewsflashes{
			TaskBatchId:     batchId,
			RespCode:        body.Code,
			RespItemIdx:     i,
			RespItemData:    string(itemData),
			ReqTimestamp:    news36krReqBody.Timestamp,
			ReqPageSize:     news36krReqBody.Param.PageSize,
			ReqPageEvent:    news36krReqBody.Param.PageEvent,
			ReqPageCallback: news36krReqBody.Param.PageCallback,
			ReqSiteId:       news36krReqBody.Param.SiteId,
			ReqPlatformId:   news36krReqBody.Param.PlatformId,
			ReqPartnerId:    news36krReqBody.PartnerId,
		}).Commit()
	}

	page := 0
	nextPageCallback := body.Data.PageCallback
	for body.Code == 0 && body.Data.HasNextPage == 1 && page < 4 {
		page += 1
		news36krReqBody = news36kr.NewFlashReq(1, nextPageCallback)
		body, err = news36kr.Crawl(news36krReqBody)
		if err != nil {
			log.Fatalf("crawl news36kr has err: %s\n", err)
			return
		}
		nextPageCallback = body.Data.PageCallback

		for i, item := range body.Data.ItemList {
			itemData, _ := json.Marshal(item)
			db.Create(&news36kr.TableNewsflashes{
				TaskBatchId:     batchId,
				RespCode:        body.Code,
				RespItemIdx:     (page * news36krReqBody.Param.PageSize) + i - 1,
				RespItemData:    string(itemData),
				ReqTimestamp:    news36krReqBody.Timestamp,
				ReqPageSize:     news36krReqBody.Param.PageSize,
				ReqPageEvent:    news36krReqBody.Param.PageEvent,
				ReqPageCallback: news36krReqBody.Param.PageCallback,
				ReqSiteId:       news36krReqBody.Param.SiteId,
				ReqPlatformId:   news36krReqBody.Param.PlatformId,
				ReqPartnerId:    news36krReqBody.PartnerId,
			}).Commit()
		}
	}
}

func InitDB(dbName string) (*gorm.DB, error) {
	passwd := os.Getenv("MYSQL001_PASSWD")
	ip := os.Getenv("MYSQL001_IP")
	port := os.Getenv("MYSQL001_PORT")

	db, err := gorm.Open(mysql.Open(fmt.Sprintf("root:%s@tcp(%s:%s)/%s", passwd, ip, port, dbName)))
	if err != nil {
		return nil, err
	}
	return db, nil
}
