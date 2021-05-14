package s3

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

const (
	DefaultMaxPartSize = 20 << 20
)

type SseKmsIdNotSetError struct {
	error
}

func NewSseKmsIdNotSetError() SseKmsIdNotSetError {
	return SseKmsIdNotSetError{errors.Errorf("%s must be set if using aws:kms encryption", SseKmsIdSetting)}
}

func (err SseKmsIdNotSetError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type Uploader struct {
	uploaderAPI          s3manageriface.UploaderAPI
	serverSideEncryption string
	SSECustomerKey       string
	SSEKMSKeyId          string
	StorageClass         string
}

func NewUploader(uploaderAPI s3manageriface.UploaderAPI, serverSideEncryption, sseCustomerKey, sseKmsKeyId, storageClass string) *Uploader {
	return &Uploader{uploaderAPI, serverSideEncryption, sseCustomerKey, sseKmsKeyId, storageClass}
}

// TODO : unit tests
func (uploader *Uploader) createUploadInput(bucket, path string, content io.Reader) *s3manager.UploadInput {
	uploadInput := &s3manager.UploadInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(path),
		Body:         content,
		StorageClass: aws.String(uploader.StorageClass),
	}

	if uploader.serverSideEncryption != "" {
		uploadInput.ServerSideEncryption = aws.String(uploader.serverSideEncryption)

		if uploader.SSECustomerKey != "" {
			uploadInput.SSECustomerKey = aws.String(uploader.SSECustomerKey)
			hash := md5.Sum([]byte(uploader.SSECustomerKey))
			uploadInput.SSECustomerKeyMD5 = aws.String(hex.EncodeToString(hash[:]))
		}

		if uploader.SSEKMSKeyId != "" {
			// Only aws:kms implies sseKmsKeyId, checked during validation
			uploadInput.SSEKMSKeyId = aws.String(uploader.SSEKMSKeyId)
		}
	}

	return uploadInput
}

func (uploader *Uploader) upload(bucket, path string, content io.Reader) error {
	input := uploader.createUploadInput(bucket, path, content)
	_, err := uploader.uploaderAPI.Upload(input)
	return errors.Wrapf(err, "failed to upload '%s' to bucket '%s'", path, bucket)
}

// CreateUploaderAPI returns an uploader with customizable concurrency
// and part size.
func CreateUploaderAPI(svc s3iface.S3API, partsize, concurrency int) s3manageriface.UploaderAPI {
	uploaderAPI := s3manager.NewUploaderWithClient(svc, func(uploader *s3manager.Uploader) {
		uploader.PartSize = int64(partsize)
		uploader.Concurrency = concurrency
	})
	return uploaderAPI
}

// TODO : unit tests
func configureServerSideEncryption(settings map[string]string) (serverSideEncryption string, sseCustomerKey string, sseKmsKeyId string, err error) {
	serverSideEncryption, _ = settings[SseSetting]
	sseCustomerKey, _ = settings[SseCSetting]
	sseKmsKeyId, _ = settings[SseKmsIdSetting]

	// Only aws:kms implies sseKmsKeyId
	if (serverSideEncryption == "aws:kms") == (sseKmsKeyId == "") {
		return "", "", "", NewSseKmsIdNotSetError()
	}
	return
}

// TODO : unit tests
func partitionStrings(strings []string, blockSize int) [][]string {
	// I've unsuccessfully tried this with interface{} but there was too much of casting
	partition := make([][]string, 0)
	for i := 0; i < len(strings); i += blockSize {
		if i+blockSize > len(strings) {
			partition = append(partition, strings[i:])
		} else {
			partition = append(partition, strings[i:i+blockSize])
		}
	}
	return partition
}

// TODO : unit tests
func configureUploader(s3Client *s3.S3, settings map[string]string) (*Uploader, error) {
	var concurrency int
	var err error
	if strConcurrency, ok := settings[UploadConcurrencySetting]; ok {
		concurrency, err = strconv.Atoi(strConcurrency)
		if err != nil {
			return nil, NewFolderError(err, "Invalid upload concurrency setting")
		}
	} else {
		return nil, NewConfiguringError(UploadConcurrencySetting)
	}

	var maxPartSize int
	if strMaxPartSize, ok := settings[MaxPartSize]; ok {
		maxPartSize, err = strconv.Atoi(strMaxPartSize)
		if err != nil {
			return nil, NewFolderError(err, "Invalid s3 max part size setting")
		}
	} else {
		maxPartSize = DefaultMaxPartSize
	}

	uploaderApi := CreateUploaderAPI(s3Client, maxPartSize, concurrency)

	serverSideEncryption, sseCustomerKey, sseKmsKeyId, err := configureServerSideEncryption(settings)
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure server side encryption")
	}

	var storageClass string
	var ok bool
	if storageClass, ok = settings[StorageClassSetting]; !ok {
		storageClass = "STANDARD"
	}
	return NewUploader(uploaderApi, serverSideEncryption, sseCustomerKey, sseKmsKeyId, storageClass), nil
}
