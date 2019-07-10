package s3

import (
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func ListKeys(awsSession *session.Session, callback func(key string), bucket string, prefix string) error {
	svc := s3.New(awsSession)
	inputparams := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(1000),
	}
	pageNum := 0
	err := svc.ListObjectsV2Pages(inputparams, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		pageNum++
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

func ListObjects(awsSession *session.Session, callback func(r *s3.ListObjectsV2Output), bucket string, prefix string) error {
	svc := s3.New(awsSession)
	inputparams := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(1000),
	}
	result, err := svc.ListObjectsV2(inputparams)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				//fmt.Println(s3.ErrCodeNoSuchBucket, aerr.Error())
			default:
				//fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			//fmt.Println(err.Error())
		}
		return nil
	}
	callback(result)
	return nil
}

func Download(awsSession *session.Session, bucket string, key string) ([]byte, error) {
	file := aws.NewWriteAtBuffer([]byte{})
	downloader := s3manager.NewDownloader(awsSession)
	downloader.Concurrency = 20
	_, err := downloader.Download(file, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	fmt.Println(downloader)
	if err != nil {
		return nil, err
	}
	return file.Bytes(), nil
}

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
