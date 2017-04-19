package exec_test

import (
	"os"
	osexec "os/exec"
	"testing"

	uuid "github.com/satori/go.uuid"
	"github.com/scgolang/exec"
)

func newTestGroups(t *testing.T, root string) *exec.Groups {
	_ = os.RemoveAll(root)
	gs, err := exec.NewGroups(root, "groups.db")
	if err != nil {
		t.Fatal(err)
	}
	return gs
}

func TestGroups(t *testing.T) {
	var (
		commandID = uuid.NewV4().String()
		commands  = []*exec.Cmd{
			&exec.Cmd{
				Cmd: osexec.Command("echo", "foo"),
				ID:  commandID,
			},
		}
		groupName = "echofoo"
		gs        = newTestGroups(t, ".data")
	)
	if err := gs.Create(groupName, commands...); err != nil {
		t.Fatal(err)
	}
	if err := gs.Wait(groupName); err != nil {
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
}

func TestGroupsOpen(t *testing.T) {
	_ = newTestGroups(t, ".echofoo")
}
