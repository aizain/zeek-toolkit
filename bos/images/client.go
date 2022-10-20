package images

import (
	"fmt"
	"github.com/baidubce/bce-sdk-go/services/bos"
	"github.com/baidubce/bce-sdk-go/services/bos/api"
	"gorm.io/gorm"
	"io"
)

type ImageClient struct {
	bos    *bos.Client
	bucket string
}

type TableTestCdcImages struct {
	gorm.Model
	TaskBatchId string `gorm:"column:task_batch_id"`
	BosEndpoint string `gorm:"column:bos_endpoint"`
	BosBucket   string `gorm:"column:bos_bucket"`
	BosKey      string `gorm:"column:bos_key"`
	Content     string `gorm:"column:content"`
	Md5         string `gorm:"column:md5"`
}

func (TableTestCdcImages) TableName() string {
	return "test_cdc_images"
}

func NewImageClient(endpoint string, ak string, sk string, bucket string) (*ImageClient, error) {
	clientConfig := bos.BosClientConfiguration{
		Ak:               ak,
		Sk:               sk,
		Endpoint:         endpoint,
		RedirectDisabled: false,
	}

	// 初始化一个BosClient
	client, err := bos.NewClientWithConfig(&clientConfig)
	if err != nil {
		fmt.Printf("get bos client err %d\n", err)
		return nil, err
	}
	return &ImageClient{
		bos:    client,
		bucket: bucket,
	}, nil
}

func (client *ImageClient) ListImages(prefix string) (*api.ListObjectsResult, error) {
	ret, err := client.bos.ListObjects(client.bucket, &api.ListObjectsArgs{
		Prefix: prefix,
	})
	if err != nil {
		fmt.Printf("list images err %d\n", err)
		return nil, err
	}
	return ret, nil
}

func (client *ImageClient) GetImage(key string) (*api.GetObjectResult, error) {
	header := make(map[string]string, 1)
	ret, err := client.bos.GetObject(client.bucket, key, header)
	if err != nil {
		fmt.Printf("get image[%s] err %d\n", key, err)
		return nil, err
	}
	return ret, nil
}

func (client *ImageClient) GetImageMeta(key string) (*api.GetObjectMetaResult, error) {
	ret, err := client.bos.GetObjectMeta(client.bucket, key)
	if err != nil {
		fmt.Printf("get image[%s] err %d\n", key, err)
		return nil, err
	}
	return ret, nil
}

func (client *ImageClient) RestoringImage(key string) error {
	err := client.bos.RestoreObject(client.bucket, key, 7, api.RESTORE_TIER_EXPEDITED)
	if err != nil {
		fmt.Printf("restoring image[%s] err %d\n", key, err)
		return err
	}
	return nil
}

func (client *ImageClient) GetImageContent(key string) ([]byte, error) {
	header := make(map[string]string, 1)
	ret, err := client.bos.GetObject(client.bucket, key, header)
	if err != nil {
		fmt.Printf("get image[%s] err %d\n", key, err)
		return nil, err
	}
	// 获取Object的读取流（io.ReadCloser）
	stream := ret.Body

	// 确保关闭Object读取流
	defer func(stream io.ReadCloser) {
		_ = stream.Close()
	}(stream)

	// 调用stream对象的Read方法处理Object
	data, err := io.ReadAll(stream)
	if err != nil {
		fmt.Printf("read image[%s] err %d\n", key, err)
		return nil, err
	}
	return data, nil
}
