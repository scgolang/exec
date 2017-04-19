package exec_test

import (
	"os"
	osexec "os/exec"
	"testing"

	uuid "github.com/satori/go.uuid"
	"github.com/scgolang/exec"
)

const (
	testGroupName = "echofoo"
)

var (
	echofooCommandID = uuid.NewV4().String()
)

func TestGroups(t *testing.T) {
	const root = ".data"

	_ = os.RemoveAll(root)

	var (
		commands = []*exec.Cmd{
			&exec.Cmd{
				Cmd: osexec.Command("echo", "foo"),
				ID:  echofooCommandID,
			},
		}
		gs = newTestGroups(t, root)
	)
	if err := gs.Create(testGroupName, commands...); err != nil {
		t.Fatal(err)
	}
	verifyEchoFoo(gs, echofooCommandID, t)
}

func TestGroupsOpen(t *testing.T) {
	gs := newTestGroups(t, ".echofoo")
	cmds, err := gs.Open(testGroupName)
	if err != nil {
		t.Fatal(err)
	}
	if expected, got := 1, len(cmds); expected != got {
		t.Fatalf("expected %d, got %d", expected, got)
	}
	verifyEchoFoo(gs, cmds[0].ID, t)
}

func newTestGroups(t *testing.T, root string) *exec.Groups {
	gs, err := exec.NewGroups(root, "groups.db")
	if err != nil {
		t.Fatal(err)
	}
	return gs
}

func verifyEchoFoo(gs *exec.Groups, commandID string, t *testing.T) {
	if err := gs.Wait(testGroupName); err != nil {
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
