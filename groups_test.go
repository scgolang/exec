package exec_test

import (
	"os"
	osexec "os/exec"
	"testing"

	ulid "github.com/imdario/go-ulid"
	"github.com/scgolang/exec"
)

func newTestGroups(t *testing.T) *exec.Groups {
	_ = os.RemoveAll(".data")
	gs, err := exec.NewGroups(".data", "groups.db")
	if err != nil {
		t.Fatal(err)
	}
	return gs
}

func TestGroups(t *testing.T) {
	var (
		commandID = ulid.New().String()
		commands  = []*exec.Cmd{
			&exec.Cmd{
				Cmd: osexec.Command("echo", "foo"),
				ID:  commandID,
			},
		}
		groupName = "echofoo"
		gs        = newTestGroups(t)
	)
	if err := gs.Create(groupName, commands...); err != nil {
		t.Fatal(err)
	}
	scanner, err := gs.Logs(commandID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !scanner.Scan() {
		t.Fatal("expected to be able to scan one line")
	}
	if expected, got := "foo", scanner.Text(); expected != got {
		t.Fatalf("expected %s, got %s", expected, got)
	}
	if err := gs.Close(groupName); err != nil {
		t.Fatal(err)
	}
}
