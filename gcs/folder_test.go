package gcs

import (
	"github.com/wal-g/storages/storage"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGSFolder(t *testing.T) {
	t.Skip("Credentials needed to run GCP tests")

	storageFolder, err := ConfigureFolder("gs://x4m-test/walg-bucket",
		nil)

	assert.NoError(t, err)

	storage.RunFolderTest(storageFolder, t)
}

func TestGSExactFolder(t *testing.T) {
	t.Skip("Credentials needed to run GCP tests")

	storageFolder, err := ConfigureFolderWithExactPrefix("gs://x4m-test//walg-bucket////strange_folder",
		nil)

	assert.NoError(t, err)

	storage.RunFolderTest(storageFolder, t)
}
