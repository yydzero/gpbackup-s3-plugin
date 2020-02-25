package s3plugin

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/alecthomas/units"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
)

var Version string

const API_VERSION = "0.4.0"

type Scope string

const (
	Master      Scope = "master"
	SegmentHost Scope = "segment_host"
	Segment     Scope = "segment"
)

// 8 MB per part, supporting a file size up to 80GB
const DownloadChunkSize = int64(units.Mebibyte) * 8
const DownloadChunkIncrement = int64(units.Mebibyte) * 2
const UploadChunkSize = int64(units.Mebibyte) * 8
const Concurrency = 8

type PluginConfig struct {
	ExecutablePath string
	Options        map[string]string
}

type S3Manager interface {
	Delete(bucket, dirPath string) error
}

type S3Plugin struct {
	manager S3Manager
	config  *PluginConfig
}

func NewS3Plugin(manager S3Manager, config *PluginConfig) *S3Plugin {
	return &S3Plugin{
		manager: manager,
		config:  config,
	}
}

func NewS3PluginProduction(c *cli.Context) (*S3Plugin, error) {
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return nil, err
	}

	prodPlugin := NewS3Plugin(
		&ProductionS3Manager{
			service: s3.New(sess),
			config:  config,
		},
		config)

	return prodPlugin, nil
}

func SetupPluginForBackup(c *cli.Context) error {
	scope := (Scope)(c.Args().Get(2))
	if scope != Master && scope != SegmentHost {
		return nil
	}

	gplog.InitializeLogging("gpbackup", "")
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	localBackupDir := c.Args().Get(1)
	_, timestamp := filepath.Split(localBackupDir)
	testFilePath := fmt.Sprintf("%s/gpbackup_%s_report", localBackupDir, timestamp)
	fileKey := GetS3Path(config.Options["folder"], testFilePath)
	file, err := os.Create(testFilePath) // dummy empty reader for probe
	defer func() {
		_ = file.Close()
	}()
	if err != nil {
		return err
	}
	_, err = uploadFile(sess, config.Options["bucket"], fileKey, file)
	return err
}

func SetupPluginForRestore(c *cli.Context) error {
	scope := (Scope)(c.Args().Get(2))
	if scope != Master && scope != SegmentHost {
		return nil
	}
	gplog.InitializeLogging("gprestore", "")
	_, err := readAndValidatePluginConfig(c.Args().Get(0))
	return err
}

func CleanupPlugin(c *cli.Context) error {
	_ = c
	return nil
}

func BackupFile(c *cli.Context) error {
	gplog.InitializeLogging("gpbackup", "")
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	filename := c.Args().Get(1)
	fileKey := GetS3Path(config.Options["folder"], filename)
	file, err := os.Open(filename)
	defer func() {
		_ = file.Close()
	}()
	if err != nil {
		return err
	}
	totalBytes, err := uploadFile(sess, config.Options["bucket"], fileKey, file)
	if err == nil {
		gplog.Verbose("Uploaded %d bytes for %s", totalBytes, fileKey)
	}
	return err
}

func RestoreFile(c *cli.Context) error {
	gplog.InitializeLogging("gprestore", "")
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	filename := c.Args().Get(1)
	fileKey := GetS3Path(config.Options["folder"], filename)
	file, err := os.Create(filename)
	defer func() {
		_ = file.Close()
	}()
	if err != nil {
		return err
	}
	_, err = downloadFile(sess, config.Options["bucket"], fileKey, file)
	if err != nil {
		_ = os.Remove(filename)
	}
	return err
}

func BackupData(c *cli.Context) error {
	gplog.InitializeLogging("gpbackup", "")
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	dataFile := c.Args().Get(1)
	fileKey := GetS3Path(config.Options["folder"], dataFile)
	totalBytes, err := uploadFile(sess, config.Options["bucket"], fileKey, os.Stdin)
	if err == nil {
		gplog.Verbose("Uploaded %d bytes for file %s", totalBytes, fileKey)
	}
	return err
}

func RestoreData(c *cli.Context) error {
	gplog.InitializeLogging("gprestore", "")
	config, sess, err := readConfigAndStartSession(c)
	if err != nil {
		return err
	}
	dataFile := c.Args().Get(1)
	fileKey := GetS3Path(config.Options["folder"], dataFile)
	totalBytes, err := downloadFile(sess, config.Options["bucket"], fileKey, os.Stdout)
	if err == nil {
		gplog.Verbose("Downloaded %d bytes for file %s", totalBytes, fileKey)
	}
	return err
}

func GetAPIVersion(c *cli.Context) {
	_ = c
	fmt.Println(API_VERSION)
}

/*
 * Helper Functions
 */

func readAndValidatePluginConfig(configFile string) (*PluginConfig, error) {
	config := &PluginConfig{}
	contents, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}
	if err = yaml.Unmarshal(contents, config); err != nil {
		return nil, err
	}
	if err = ValidateConfig(config); err != nil {
		return nil, err
	}
	return config, nil
}

func ValidateConfig(config *PluginConfig) error {
	requiredKeys := []string{"bucket", "folder"}
	for _, key := range requiredKeys {
		if config.Options[key] == "" {
			return fmt.Errorf("%s must exist in plugin configuration file", key)
		}
	}

	if config.Options["aws_access_key_id"] == "" {
		if config.Options["aws_secret_access_key"] != "" {
			return fmt.Errorf("aws_access_key_id must exist in plugin configuration file if aws_secret_access_key does")
		}
	} else if config.Options["aws_secret_access_key"] == "" {
		return fmt.Errorf("aws_secret_access_key must exist in plugin configuration file if aws_access_key_id does")
	}

	if config.Options["region"] == "" {
		if config.Options["endpoint"] == "" {
			return fmt.Errorf("region or endpoint must exist in plugin configuration file")
		}
		config.Options["region"] = "unused"
	}

	return nil
}

func readConfigAndStartSession(c *cli.Context) (*PluginConfig, *session.Session, error) {
	configPath := c.Args().Get(0)
	config, err := readAndValidatePluginConfig(configPath)
	if err != nil {
		return nil, nil, err
	}
	disableSSL := !ShouldEnableEncryption(config)

	awsConfig := aws.NewConfig().
		WithRegion(config.Options["region"]).
		WithEndpoint(config.Options["endpoint"]).
		WithS3ForcePathStyle(true).
		WithDisableSSL(disableSSL)

	// Will use default credential chain if none provided
	if config.Options["aws_access_key_id"] != "" {
		awsConfig = awsConfig.WithCredentials(
			credentials.NewStaticCredentials(
				config.Options["aws_access_key_id"],
				config.Options["aws_secret_access_key"], ""))
	}

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, nil, err
	}
	return config, sess, nil
}

func ShouldEnableEncryption(config *PluginConfig) bool {
	isOff := strings.EqualFold(config.Options["encryption"], "off")
	return !isOff
}

func uploadFile(sess *session.Session, bucket string, fileKey string, file *os.File) (int64, error) {
	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.PartSize = UploadChunkSize
		u.Concurrency = Concurrency
	})
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileKey),
		Body:   bufio.NewReaderSize(file, int(UploadChunkSize) * Concurrency),
	})
	if err != nil {
		return 0, err
	}
	return getFileSize(uploader.S3, bucket, fileKey)
}

type chunk struct {
	chunkNo   int
	startByte int64
	endByte   int64
}

/*
 * Performs ranged requests for the file while exploiting parallelism between the copy and download tasks
 */
func downloadFileInParallel(downloader *s3manager.Downloader, totalBytes int64,
	bucket string, fileKey string, file *os.File) (int64, error) {

	var finalErr error
	waitGroup := sync.WaitGroup{}
	noOfChunks := int(math.Ceil(float64(totalBytes) / float64(DownloadChunkSize)))
	downloadBuffers := make([]*aws.WriteAtBuffer, noOfChunks)
	copyChannel := make([]chan int, noOfChunks)
	jobs := make(chan chunk, noOfChunks)
	for i := range copyChannel {
		copyChannel[i] = make(chan int)
	}

	go func() {
		for i := range copyChannel {
			currChunk := <- copyChannel[i]
			written, err := io.Copy(file, bytes.NewReader(downloadBuffers[currChunk].Bytes()))
			if err != nil {
				finalErr = err
			}
			gplog.Verbose("Copied %d bytes for chunk %d", written, currChunk)
			waitGroup.Done()
			close(copyChannel[i])
		}
	}()

	for i := 0; i < Concurrency; i++ {
		go func(id int) {
			for j := range jobs {
				chunkBytes, err := downloader.Download(
					downloadBuffers[j.chunkNo],
					&s3.GetObjectInput{
						Bucket: aws.String(bucket),
						Key:    aws.String(fileKey),
						Range:  aws.String(fmt.Sprintf("bytes=%d-%d", j.startByte, j.endByte)),
					})
				if err != nil {
					finalErr = err
				}
				gplog.Verbose("Worker %d Downloaded %d bytes for chunk %d", id, chunkBytes, j.chunkNo)
				copyChannel[j.chunkNo] <- j.chunkNo
			}
		}(i)
	}

	startByte := int64(0)
	endByte := DownloadChunkSize - 1
	done := false
	for currentChunkNo := 0; currentChunkNo < noOfChunks || !done; currentChunkNo++ {
		if endByte >= totalBytes {
			endByte = totalBytes - 1
			done = true
		}
		downloadBuffers[currentChunkNo] = &aws.WriteAtBuffer{GrowthCoeff: 2}
		jobs <- chunk{
			currentChunkNo,
			startByte,
			endByte,
		}
		waitGroup.Add(1)
		startByte += DownloadChunkSize + int64(currentChunkNo) * DownloadChunkIncrement
		endByte += DownloadChunkSize + int64(currentChunkNo) * DownloadChunkIncrement
	}

	waitGroup.Wait()
	return totalBytes, finalErr
}

/*
 * Performs ranged requests for the file while exploiting parallelism between the copy and download tasks
 */
func downloadFile(sess *session.Session, bucket string, fileKey string, file *os.File) (int64, error) {
	downloader := s3manager.NewDownloader(sess)

	totalBytes, err := getFileSize(downloader.S3, bucket, fileKey)
	if err != nil {
		return 0, err
	}
	gplog.Verbose("File %s size = %d bytes", fileKey, totalBytes)
	if totalBytes <= DownloadChunkSize {
		_, err = downloader.Download(
			file,
			&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(fileKey),
			})
	} else {
		return downloadFileInParallel(downloader, totalBytes, bucket, fileKey, file)
	}

	return totalBytes, err
}

func getFileSize(S3 s3iface.S3API, bucket string, fileKey string) (int64, error) {
	req, resp := S3.HeadObjectRequest(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileKey),
	})
	err := req.Send()

	if err != nil {
		return 0, err
	}
	return *resp.ContentLength, nil
}

func GetS3Path(folder string, path string) string {
	/*
		a typical path for an already-backed-up file will be stored in a
		parent directory of a segment, and beneath that, under a datestamp/timestamp/ hierarchy. We assume the incoming path is a long absolute one.
		For example from the test bench:
		  testdir_for_del="/tmp/testseg/backups/$current_date_for_del/$time_second_for_del"
		  testfile_for_del="$testdir_for_del/testfile_$time_second_for_del.txt"

		Therefore, the incoming path is relevant to S3 in only the last four segments,
		which indicate the file and its 2 date/timestamp parents, and the grandparent "backups"
	*/
	pathArray := strings.Split(path, "/")
	lastFour := strings.Join(pathArray[(len(pathArray)-4):], "/")
	return fmt.Sprintf("%s/%s", folder, lastFour)
}

func (plugin S3Plugin) Delete(c *cli.Context) error {
	timestamp := c.Args().Get(1)
	if timestamp == "" {
		return errors.New("delete requires a <timestamp>")
	}

	if !IsValidTimestamp(timestamp) {
		return fmt.Errorf("delete requires a <timestamp> with format YYYYMMDDHHMMSS, but received: %s", timestamp)
	}

	date := timestamp[0:8]
	// note that "backups" is a directory is a fact of how we save, choosing
	// to use the 3 parent directories of the source file. That becomes:
	// <s3folder>/backups/<date>/<timestamp>
	deletePath := filepath.Join(plugin.config.Options["folder"], "backups", date, timestamp)
	bucket := plugin.config.Options["bucket"]

	return plugin.manager.Delete(bucket, deletePath)
}

func IsValidTimestamp(timestamp string) bool {
	timestampFormat := regexp.MustCompile(`^([0-9]{14})$`)
	return timestampFormat.MatchString(timestamp)
}

type ProductionS3Manager struct {
	service *s3.S3
	config  *PluginConfig
}

func (d ProductionS3Manager) Delete(bucket, dirPath string) error {
	iter := s3manager.NewDeleteListIterator(d.service, &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(dirPath),
	})
	batcher := s3manager.NewBatchDeleteWithClient(d.service)
	return batcher.Delete(aws.BackgroundContext(), iter)
}
