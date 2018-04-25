package s3plugin

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/urfave/cli"
	yaml "gopkg.in/yaml.v2"
)

var Version string

func SetupPluginForBackup(c *cli.Context) error {
	config, session, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	if err = validateConfig(config); err != nil {
		return err
	}
	backupDir := c.Args().Get(1)
	_, timestamp := filepath.Split(backupDir)
	testFilePath := fmt.Sprintf("%s/gpbackup_%s_report", backupDir, timestamp)
	fileKey := GetS3Path(config.Options["backupdir"], testFilePath)
	reader := strings.NewReader("")
	if err = uploadFile(session, config.Options["bucket"], fileKey, reader); err != nil {
		return err
	}
	return nil
}

func SetupPluginForRestore(c *cli.Context) error {
	config, err := readPluginConfig(c.Args().Get(0))
	if err != nil {
		return err
	}
	if err = validateConfig(config); err != nil {
		return err
	}
	backupDir := c.Args().Get(1)
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return err
	}
	return nil
}

func CleanupPlugin(c *cli.Context) error {
	return nil
}

func BackupFile(c *cli.Context) error {
	config, session, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	filename := c.Args().Get(1)
	fileKey := GetS3Path(config.Options["backupdir"], filename)
	reader, err := os.Open(filename)
	if err != nil {
		return err
	}
	if err = uploadFile(session, config.Options["bucket"], fileKey, reader); err != nil {
		return err
	}
	if err = os.Remove(filename); err != nil {
		return err
	}
	return nil
}

func RestoreFile(c *cli.Context) error {
	config, session, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	filename := c.Args().Get(1)
	fileKey := GetS3Path(config.Options["backupdir"], filename)
	writer, err := os.Create(filename)
	if err != nil {
		return err
	}
	if err = downloadFile(session, config.Options["bucket"], fileKey, writer); err != nil {
		return err
	}
	return nil
}

func BackupData(c *cli.Context) error {
	config, session, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	dataFile := c.Args().Get(1)
	fileKey := GetS3Path(config.Options["backupdir"], dataFile)
	reader := bufio.NewReader(os.Stdin)
	if err = uploadFile(session, config.Options["bucket"], fileKey, reader); err != nil {
		return err
	}
	return nil
}

func RestoreData(c *cli.Context) error {
	config, session, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	dataFile := c.Args().Get(1)
	fileKey := GetS3Path(config.Options["backupdir"], dataFile)
	if err = downloadFile(session, config.Options["bucket"], fileKey, os.Stdout); err != nil {
		return err
	}
	return nil
}

func GetAPIVersion(c *cli.Context) {
	fmt.Println("0.1.0")
}

/*
 * Helper Functions
 */
type PluginConfig struct {
	ExecutablePath string
	Options        map[string]string
}

func readPluginConfig(configFile string) (*PluginConfig, error) {
	config := &PluginConfig{}
	contents, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(contents, config); err != nil {
		return nil, err
	}
	return config, nil
}

func validateConfig(config *PluginConfig) error {
	required_keys := []string{"aws_access_key_id", "aws_secret_access_key", "region", "bucket", "backupdir"}
	for _, key := range required_keys {
		if config.Options[key] == "" {
			return fmt.Errorf("%s must exist in plugin configuration file", key)
		}
	}
	return nil
}

func readConfigAndStartSession(c *cli.Context) (*PluginConfig, *session.Session, error) {
	configPath := c.Args().Get(0)
	config, err := readPluginConfig(configPath)
	if err != nil {
		return nil, nil, err
	}
	region := config.Options["region"]
	credentials := credentials.NewStaticCredentials(config.Options["aws_access_key_id"], config.Options["aws_secret_access_key"], "")
	session, err := session.NewSession(&aws.Config{
		Region:      &region,
		Credentials: credentials,
	})
	if err != nil {
		return nil, nil, err
	}
	return config, session, nil
}

func uploadFile(session *session.Session, bucket string, fileKey string, fileReader io.Reader) error {
	uploader := s3manager.NewUploader(session)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileKey),
		Body:   fileReader,
	})
	return err
}

func downloadFile(session *session.Session, bucket string, fileKey string, fileWriter io.Writer) error {
	buff := &aws.WriteAtBuffer{}
	downloader := s3manager.NewDownloader(session)
	_, err := downloader.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileKey),
	})
	if err != nil {
		return err
	}
	_, err = io.Copy(fileWriter, bytes.NewReader(buff.Bytes()))
	return err
}

func GetS3Path(backupDir string, path string) string {
	pathArray := strings.Split(path, "/")
	return fmt.Sprintf("%s/%s", backupDir, strings.Join(pathArray[(len(pathArray)-4):], "/"))
}
