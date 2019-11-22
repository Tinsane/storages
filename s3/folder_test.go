package s3

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/storages/storage"
	"testing"
)

func TestS3Folder(t *testing.T) {
	t.Skip("Credentials needed to run S3 tests")

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	storageFolder, err := ConfigureFolder(waleS3Prefix,
		map[string]string{
			EndpointSetting: "http://s3.mdst.yandex.net/",
		})

	assert.NoError(t, err)

	storage.RunFolderTest(storageFolder, t)
}
func TestS3FolderEndpointSource(t *testing.T) {
	t.Skip("Credentials needed to run S3 tests")

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	storageFolder, err := ConfigureFolder(waleS3Prefix,
		map[string]string{
			EndpointSetting: "http://s3.mdst.yandex.net/",
			EndpointSourceSetting: "http://localhost:80/",
			//EndpointSourceSetting: "http://s3.mds.yandex.net/hostname",
			AccessKeySetting: "AKIAIOSFODNN7EXAMPLE",
			SecretKeySetting: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			UploadConcurrencySetting: "1",
			ForcePathStyleSetting: "True",
		})

	assert.NoError(t, err)

	storage.RunFolderTest(storageFolder, t)
}
