package s3

import (
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// ListKeys ...
//
// Lists all keys for a specific bucket and prefix with paging and
// returns each key via a callback arg
func ListKeys(awsSession *session.Session, callback func(key string), bucket string, prefix string) error {
	svc := s3.New(awsSession)
	inputparams := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(1000),
	}
	err := svc.ListObjectsV2Pages(inputparams, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, value := range page.Contents {
			callback(*value.Key)
		}
		return true
	})
	if err != nil {
		return err
	}
	return nil
}

// ListObjects ...
//
// Lists all keys for a specific bucket and prefix without paging and
// returns each key via a callback arg
// returns the first 1000 keys and no more
func ListObjects(awsSession *session.Session, callback func(key string), bucket string, prefix string) error {
	svc := s3.New(awsSession)
	inputparams := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(1000),
	}
	result, err := svc.ListObjectsV2(inputparams)
	if err != nil {
		return err
	}
	// skip over the first value as it is the prefix
	for i := 1; i < len(result.Contents); i++ {
		callback(*result.Contents[i].Key)
	}
	return nil
}

// Download ...
//
// Downloads an object
func Download(awsSession *session.Session, bucket string, key string) ([]byte, error) {
	file := aws.NewWriteAtBuffer([]byte{})
	downloader := s3manager.NewDownloader(awsSession)
	downloader.Concurrency = 20
	_, err := downloader.Download(file, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return file.Bytes(), nil
}

// Upload ...
//
// Uploads an object
func Upload(file io.Reader, awsSession *session.Session, bucket string, key string) error {
	uploader := s3manager.NewUploader(awsSession)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		return err
	}
	return nil
}
