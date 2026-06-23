package main_test

import (
	"os"
	"os/exec"
	"testing"
)

func TestMainCompiles(t *testing.T) {
	if os.Getenv("GO_TEST_COMPILE_MAIN") == "1" {
		return
	}
	cmd := exec.Command("go", "build", "-o", os.DevNull, "./cmd/notification-fanout")
	cmd.Dir = "../.."
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build main: %v\n%s", err, out)
	}
}
