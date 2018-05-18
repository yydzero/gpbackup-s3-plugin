package s3plugin_test

import (
	"testing"

	"github.com/greenplum-db/gpbackup-s3-plugin/s3plugin"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "s3_plugin tests")
}

var _ = Describe("s3_plugin tests", func() {
	Context("GetS3Path", func() {
		It("", func() {
			folder := "s3/Dir"
			path := "/tmp/datadir/gpseg-1/backups/20180101/2018010101010101/backup_file"
			newPath := s3plugin.GetS3Path(folder, path)
			expectedPath := "s3/Dir/backups/20180101/2018010101010101/backup_file"
			Expect(newPath).To(Equal(expectedPath))
		})
	})

})
