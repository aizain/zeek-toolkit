package main

import (
	"encoding/base64"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"os"
	"sync"
	"time"
	"zeek/toolkit/bos/images"
	"zeek/toolkit/bos/videos"
)

func main() {

	batchId := time.Now().Format("200601021504")
	bucket := os.Args[1]
	prefix := os.Args[2]
	//loadImages(batchId, bucket, prefix)
	loadVideos(batchId, bucket, prefix)
}

func loadVideos(batchId string, bucket string, prefix string) {
	// 获取 bos 链接
	client, err := videos.NewClient(
		os.Getenv("BOS_ENDPOINT"),
		os.Getenv("BOS_AK"),
		os.Getenv("BOS_SK"),
		bucket,
	)
	if err != nil {
		log.Fatal("get bos client err" + err.Error())
		return
	}

	list, err := client.List(prefix)
	if err != nil {
		log.Fatal("list err" + err.Error())
		return
	}

	// 初始化数据库连接
	db, err := InitDB("cdc")
	if err != nil {
		log.Fatalf("init db has err: %s\n", err)
		return
	}
	// bos数据入库
	for _, objType := range list.Contents {
		meta, err := client.GetMeta(objType.Key)
		if err != nil {
			log.Fatal("get meta err" + err.Error())
			return
		}

		content, err := client.GetContent(objType.Key)
		if err != nil {
			log.Fatal("1 get err" + err.Error())
			return
		}

		db.Create(&videos.TableTestCdcVideos{
			TaskBatchId: batchId,
			BosEndpoint: os.Getenv("BOS_ENDPOINT"),
			BosBucket:   bucket,
			BosKey:      objType.Key,
			Md5:         meta.ContentMD5,
			Length:      meta.ContentLength,
			Content:     base64.StdEncoding.EncodeToString(content),
		}).Commit()
	}

}

func loadImages(batchId string, bucket string, prefix string) {
	// 获取图片 bos 链接
	client, err := images.NewImageClient(
		os.Getenv("BOS_ENDPOINT"),
		os.Getenv("BOS_AK"),
		os.Getenv("BOS_SK"),
		bucket,
	)
	if err != nil {
		log.Fatal("get images bos client err" + err.Error())
		return
	}

	imagesRet, err := client.ListImages(prefix)
	if err != nil {
		log.Fatal("list images err" + err.Error())
		return
	}

	// 初始化数据库连接
	db, err := InitDB("cdc")
	if err != nil {
		log.Fatalf("init crawl_36kr db has err: %s\n", err)
		return
	}
	// bos图片数据入库
	wg := sync.WaitGroup{}

	for _, objType := range imagesRet.Contents {
		meta, err := client.GetImageMeta(objType.Key)
		if err != nil {
			log.Fatal("get images meta err" + err.Error())
			return
		}

		if meta.StorageClass == "ARCHIVE" && (meta.BceRestore == "" || meta.BceRestore == "ongoing-request=\"true\"") {
			if meta.BceRestore == "" {
				if err = client.RestoringImage(objType.Key); err != nil {
					log.Fatal("restoring images err" + err.Error())
					return
				}
			}
			meta, err = client.GetImageMeta(objType.Key)
			if err != nil {
				log.Fatal("get images meta err" + err.Error())
				return
			}

			wg.Add(1)
			time.Sleep(10 * time.Second)
			go func() {
				defer wg.Done()

				var index int
				for meta.BceRestore == "" || meta.BceRestore == "ongoing-request=\"true\"" {
					meta, err = client.GetImageMeta(objType.Key)
					if err != nil {
						log.Fatal("2 get images err" + err.Error())
						return
					}
					if index%14 == 0 {
						log.Printf("wait image[%s] restore %d....", objType.Key, index)
					}
					time.Sleep(5 * time.Minute)
					index++
				}
				log.Printf("restore image[%s] ok %d", objType.Key, index)

				content, err := client.GetImageContent(objType.Key)
				if err != nil {
					log.Fatal("3 get images content err" + err.Error())
					return
				}
				db.Create(&images.TableTestCdcImages{
					TaskBatchId: batchId,
					BosEndpoint: os.Getenv("BOS_ENDPOINT"),
					BosBucket:   bucket,
					BosKey:      objType.Key,
					Md5:         meta.ContentMD5,
					Content:     base64.StdEncoding.EncodeToString(content),
				}).Commit()
			}()
		} else {
			content, err := client.GetImageContent(objType.Key)
			if err != nil {
				log.Fatal("1 get images err" + err.Error())
				return
			}
			db.Create(&images.TableTestCdcImages{
				TaskBatchId: batchId,
				BosEndpoint: os.Getenv("BOS_ENDPOINT"),
				BosBucket:   bucket,
				BosKey:      objType.Key,
				Md5:         meta.ContentMD5,
				Content:     base64.StdEncoding.EncodeToString(content),
			}).Commit()
		}

	}

	wg.Wait()
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
