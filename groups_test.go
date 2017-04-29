package exec_test

import (
	"os"
	osexec "os/exec"
	"path/filepath"
	"testing"

	"github.com/scgolang/exec"
)

func TestGroupsCreate(t *testing.T) {
	var (
		groupName = "echofoo"
		root      = filepath.Join("testdata", "."+t.Name())
	)
	_ = os.RemoveAll(root)

	var (
		cmds = []*osexec.Cmd{
			osexec.Command("echo", "foo"),
		}
		gs = newTestGroups(t, root)
	)
	if err := gs.Create(groupName, cmds...); err != nil {
		t.Fatal(err)
	}
	verifyEchoFoo(gs, groupName, cmds[0], t)
}

func TestGroupsOpen(t *testing.T) {
	var (
		groupName = "echofoo"
		gs        = newTestGroups(t, "."+t.Name())
	)
	cmds, err := gs.Open(groupName)
	if err != nil {
		t.Fatal(err)
	}
	if expected, got := 1, len(cmds); expected != got {
		t.Fatalf("expected %d, got %d", expected, got)
	}
	verifyEchoFoo(gs, groupName, cmds[0], t)
}

func newTestGroups(t *testing.T, root string) *exec.Groups {
	gs, err := exec.NewGroups(root, "groups.db")
	if err != nil {
		t.Fatal(err)
	}
	return gs
}

func verifyEchoFoo(gs *exec.Groups, groupName string, cmd *osexec.Cmd, t *testing.T) {
	if err := gs.Wait(groupName); err != nil {
		t.Fatal(err)
	}
	scanner, closer, err := gs.Logs(groupName, cmd, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closer.Close() }()

	if !scanner.Scan() {
		t.Fatal("expected to be able to scan one line")
	}
	if expected, got := "foo", scanner.Text(); expected != got {
		t.Fatalf("expected %s, got %s", expected, got)
	}
}

func TestGroupsRemove(t *testing.T) {
	var (
		groupName = "greps"
		root      = filepath.Join("testdata", "."+t.Name())
	)

	_ = os.RemoveAll(root)

	var (
		commands = []*osexec.Cmd{
			osexec.Command("grep", "foo"),
			osexec.Command("grep", "bar"),
			osexec.Command("grep", "baz"),
		}
		gs = newTestGroups(t, root)
	)
	if err := gs.Create(groupName, commands...); err != nil {
		t.Fatal(err)
	}
	if err := gs.Remove(groupName, commands[1]); err != nil {
		t.Fatal(err)
	}
	cmds, ok := gs.Commands(groupName)
	if !ok {
		t.Fatal("group does not exist")
	}
	if expected, got := 2, len(cmds); expected != got {
		t.Fatalf("expected %d commands, got %d", expected, got)
	}
	for i, cmd := range []*osexec.Cmd{
		osexec.Command("grep", "foo"),
		osexec.Command("grep", "baz"),
	} {
		if expected, got := getCommandID(cmd, t), getCommandID(cmds[i], t); expected != got {
			t.Fatalf("expected command ID %s, got %s", expected, got)
		}
	}
}

func TestGroupsRemoveAll(t *testing.T) {
	const (
		groupName = "greps"
		root      = ".data"
	)

	_ = os.RemoveAll(root)

	var (
		commands = []*osexec.Cmd{
			osexec.Command("grep", "foo"),
			osexec.Command("grep", "bar"),
			osexec.Command("grep", "baz"),
		}
		gs = newTestGroups(t, root)
	)
	if err := gs.Create(groupName, commands...); err != nil {
		t.Fatal(err)
	}
	if err := gs.Remove(groupName); err != nil {
		t.Fatal(err)
	}
	cmds, ok := gs.Commands(groupName)
	if !ok {
		t.Fatal("group does not exist")
	}
	if expected, got := 0, len(cmds); expected != got {
		t.Fatalf("expected %d commands, got %d", expected, got)
	}
}

func getCommandID(cmd *osexec.Cmd, t *testing.T) string {
	cid, err := exec.GetCmdID(cmd)
	if err != nil {
		t.Fatal(err)
	}
	return cid
}
