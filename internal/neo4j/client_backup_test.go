package neo4j

import (
	"testing"
)

func TestBuildBackupArgsIncludesNewFlags(t *testing.T) {
	client := &Client{}
	options := BackupOptions{
		Compress:                true,
		Verify:                  true,
		ParallelDownload:        true,
		RemoteAddressResolution: true,
	}

	args := client.buildBackupArgs("neo4j", "backup", "/data/backups", options)

	assertContains(t, args, "--parallel-download")
	assertContains(t, args, "--remote-address-resolution")
	assertContains(t, args, "--compress")
	assertContains(t, args, "--check-consistency")
}

func assertContains(t *testing.T, args []string, expected string) {
	t.Helper()
	for _, a := range args {
		if a == expected {
			return
		}
	}
	t.Fatalf("expected args to contain %q, got %v", expected, args)
}
