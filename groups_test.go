package exec_test

import (
	"os"
	osexec "os/exec"
	"path/filepath"
	"testing"

	ulid "github.com/imdario/go-ulid"
	"github.com/scgolang/exec"
)

func newTestGroups(t *testing.T) *exec.Groups {
	_ = os.Remove(filepath.Join(exec.DataDir, exec.GroupsDB))
	gs, err := exec.NewGroups(".")
	if err != nil {
		t.Fatal(err)
	}
	return gs
}

func TestGroups(t *testing.T) {
	gs := newTestGroups(t)
	if err := gs.Create("echofoo"); err != nil {
		t.Fatal(err)
	}
	cid := ulid.New().String()

	if err := gs.Start(cid, osexec.Command("echo", "foo")); err != nil {
		t.Fatal(err)
	}
}
