package sh

import (
	"github.com/pkg/sftp"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"golang.org/x/crypto/ssh"
	"io"
	"io/ioutil"
	"os"
	"fmt"
	"path/filepath"
)

type Folder struct {
	client SftpClient
	path string
}

const (
	Port = "SSH_PORT"
	Password = "SSH_PASSWORD"
	Username = "SSH_USERNAME"
	PrivateKeyPath = "SSH_PRIVATE_KEY_PATH"
)

var SettingsList = []string{
	Port,
	Password,
	Username,
	PrivateKeyPath,
};

func NewFolderError(err error, format string, args ...interface{}) storage.Error {
	return storage.NewError(err, "SSH", format, args...)
}

func ConfigureFolder(prefix string, settings map[string]string) (storage.Folder, error) {
	host, path, err := storage.ParsePrefixAsURL(prefix)

	if err != nil {
		return nil, err
	}

	user := settings[Username]
	pass := settings[Password]
	port := settings[Port]
	pkeyPath := settings[PrivateKeyPath]

	if port == "" {
		port = "22"
	}

	authMethods := []ssh.AuthMethod{}
	if pkeyPath != "" {
		pkey, err := ioutil.ReadFile(pkeyPath)
		if err != nil {
			return nil, NewFolderError(err, "Unable to read private key: %v", err)
		}

		signer, err := ssh.ParsePrivateKey(pkey)
		if err != nil {
			return nil, NewFolderError(err, "Unable to parse private key: %v", err)
		}

		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}

	if pass != "" {
		authMethods = append(authMethods, ssh.Password(pass))
	}

	config := &ssh.ClientConfig{
		User: user,
		Auth: authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	address := fmt.Sprint(host, ":", port)
	sshClient, err := ssh.Dial("tcp", address, config)
	if err != nil {
		return nil, NewFolderError(err, "Fail connect via ssh. Address: %s", address)
	}

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		return nil, NewFolderError(err, "Fail connect via sftp. Address: %s", address)
	}

	return &Folder{
		extend(sftpClient), path,
	}, nil
}

// TODO close ssh and sftp connection
func closeConnection(client io.Closer)  {
	err := client.Close()
	if err != nil {
		tracelog.WarningLogger.FatalOnError(err)
	}
}

func (folder *Folder) GetPath() string {
	return folder.path
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	client := folder.client
	path := folder.path

	filesInfo, err := client.ReadDir(folder.path)

	if os.IsNotExist(err) {
		// Folder does not exists, it means where are no objects in folder
		tracelog.InfoLogger.Println("\tskipped " + folder.path + ": " + err.Error())
		err = nil
		return
	}

	if err != nil {
		return nil, nil,
			NewFolderError(err, "Fail read folder '%s'", path)
	}

	for _, fileInfo := range filesInfo {
		if fileInfo.IsDir() {
			folder := &Folder{
				folder.client,
				client.Join(path, fileInfo.Name()),
			}
			subFolders = append(subFolders, folder)
			// Folder is not object, just skip it
			continue
		}

		object := storage.NewLocalObject(
			fileInfo.Name(),
			fileInfo.ModTime(),
			fileInfo.Size(),
		)
		objects = append(objects, object)
	}

	return
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error { 
	client := folder.client

	for _, relativePath := range objectRelativePaths {
		path := client.Join(folder.path, relativePath)

		stat, err := client.Stat(path)
		if err != nil {
			return NewFolderError(err, "Fail to get object stat '%s': %v", path, err)
		}

		// Do not try to remove directory. It may be not empty. TODO: remove if empty
		if stat.IsDir() {
			continue
		}

		err = client.Remove(path)
		if err != nil {
			return NewFolderError(err, "Fail delete object '%s': %v", path, err)
		}
	}

	return nil
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error)  {
	path := filepath.Join(folder.path, objectRelativePath)
	_, err := folder.client.Stat(path)

	if err != nil {
		return false, NewFolderError(
			err, "Fail check object existence '%s'", path,
		)
	}

	return true, nil
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return &Folder{
		folder.client,
		folder.client.Join(folder.path, subFolderRelativePath),
	}
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	path := folder.client.Join(folder.path, objectRelativePath)
	file, err := folder.client.OpenFile(path)

	if err != nil {
		return nil, storage.NewObjectNotFoundError(path)
	}

	return file, nil
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	client := folder.client
	absolutePath := filepath.Join(folder.path, name)

	dirPath := filepath.Dir(absolutePath)
	err := client.Mkdir(dirPath)
	if err != nil {
		return NewFolderError(
			err, "Fail to create directory '%s'", 
			dirPath,
		)
	}

	file, err := client.CreateFile(absolutePath)
	if err != nil {
		return NewFolderError(
			err, "Fail to create file '%s'", 
			absolutePath,
		)
	}

	_, err = io.Copy(file, content)
	if err != nil {
		return NewFolderError(
			err, "Fail write content to file '%s'", 
			absolutePath,
		)
	}

	return nil
}
