// +build gpbackup_s3_plugin

package main

import (
	"log"
	"os"

	"github.com/greenplum-db/gpbackup-s3-plugin/s3plugin"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	cli.VersionFlag = cli.BoolFlag{
		Name:  "version",
		Usage: "print version of gpbackup_s3_plugin",
	}
	app.Version = s3plugin.Version
	app.Usage = "S3 plugin for gpbackup and gprestore"

	app.Commands = []cli.Command{
		{
			Name:   "setup_plugin_for_backup",
			Action: s3plugin.SetupPluginForBackup,
		},
		{
			Name:   "setup_plugin_for_restore",
			Action: s3plugin.SetupPluginForRestore,
		},
		{
			Name:   "cleanup_plugin_for_backup",
			Action: s3plugin.CleanupPlugin,
		},
		{
			Name:   "cleanup_plugin_for_restore",
			Action: s3plugin.CleanupPlugin,
		},
		{
			Name:   "backup_file",
			Action: s3plugin.BackupFile,
		},
		{
			Name:   "restore_file",
			Action: s3plugin.RestoreFile,
		},
		{
			Name:   "backup_data",
			Action: s3plugin.BackupData,
		},
		{
			Name:   "restore_data",
			Action: s3plugin.RestoreData,
		},
		{
			Name:   "plugin_api_version",
			Action: s3plugin.GetAPIVersion,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
