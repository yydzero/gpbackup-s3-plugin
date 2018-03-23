// +build gp_s3plugin

package main

import (
	"log"
	"os"

	"github.com/greenplum-db/gpbackup-s3-plugin/s3plugin"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()

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
