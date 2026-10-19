package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

func key(s string) tea.KeyMsg {
	switch s {
	case "enter":
		return tea.KeyMsg{Type: tea.KeyEnter}
	case "ctrl+c":
		return tea.KeyMsg{Type: tea.KeyCtrlC}
	case "tab":
		return tea.KeyMsg{Type: tea.KeyTab}
	case "backspace":
		return tea.KeyMsg{Type: tea.KeyBackspace}
	case "esc":
		return tea.KeyMsg{Type: tea.KeyEsc}
	default:
		return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
	}
}

func TestInviteAcceptAndReject(t *testing.T) {
	s := newScreen(nil)
	s, _, _ = applyNote(s, note{kind: noteInvite, key: "alice", id: "req-1"})
	if len(s.rows) != 1 || s.rows[0].key != "alice" || s.rows[0].invite != "req-1" {
		t.Fatalf("row %+v", s.rows)
	}
	if len(s.toasts) != 1 || !strings.Contains(s.toasts[0].text, "incoming request from alice") {
		t.Fatalf("toast %+v", s.toasts)
	}
	if strings.Contains(strings.Join(s.lines, "\n"), "incoming request from alice") {
		t.Fatalf("invite leaked into transcript %v", s.lines)
	}
	_, in, _ := reduce(s, key("a"))
	if in.line != "accept req-1" {
		t.Fatalf("accept %q", in.line)
	}
	_, in, _ = reduce(s, key("r"))
	if in.line != "reject req-1" {
		t.Fatalf("reject %q", in.line)
	}
}

func TestEnterConnectsSelectedPeer(t *testing.T) {
	s := newScreen(nil)
	s, _, _ = applyNote(s, note{kind: noteLookup, key: "bob", state: "available", address: "127.0.0.1:9002", text: "peer: bob"})
	next, in, _ := reduce(s, key("enter"))
	if in.line != "connect bob" {
		t.Fatalf("line %q", in.line)
	}
	if next.phase != phaseConnecting {
		t.Fatalf("phase %d", next.phase)
	}
}

func TestEnterFocusesLiveChat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &app{ctx: ctx, cancel: cancel, maxChats: 4}
	a.initRegistry()
	_ = attachPipe(t, a.registry, "req-bob", "bob")
	s := newScreen(a)
	s.phase = phaseChat
	s, _, _ = applyNote(s, note{kind: noteLookup, key: "bob", state: "available", address: "127.0.0.1:9", text: "peer: bob"})
	_, in, _ := reduce(s, key("enter"))
	if in.line != "/focus req-bob" {
		t.Fatalf("line %q", in.line)
	}
}

func TestChatEnterSendsTheLine(t *testing.T) {
	s := newScreen(nil)
	s.phase = phaseChat
	s.focus = focusCompose
	s.compose = "hello from alice"
	s.cursor = len([]rune(s.compose))
	_, in, _ := reduce(s, key("enter"))
	if in.line != "hello from alice" {
		t.Fatalf("line %q", in.line)
	}
	s.compose = "/quit"
	s.cursor = len("/quit")
	_, in, _ = reduce(s, key("enter"))
	if in.line != "/quit" {
		t.Fatalf("quit line %q", in.line)
	}
}

func TestLobbyComposeKeepsCommands(t *testing.T) {
	s := newScreen(nil)
	s.focus = focusCompose
	s.compose = "lookup bob"
	s.cursor = len(s.compose)
	_, in, _ := reduce(s, key("enter"))
	if in.line != "lookup bob" {
		t.Fatalf("line %q", in.line)
	}
}

func TestComposeKeepsSpaces(t *testing.T) {
	s := newScreen(nil)
	s.focus = focusCompose
	for _, ch := range []string{"h", "i", " ", "b", "o", "b"} {
		s, _, _ = reduce(s, key(ch))
	}
	if s.compose != "hi bob" {
		t.Fatalf("compose %q", s.compose)
	}
}

func TestPeerAndSelfLines(t *testing.T) {
	s := newScreen(nil)
	s, _, _ = applyNote(s, note{kind: noteSelf, text: "hello from alice"})
	s, _, _ = applyNote(s, note{kind: notePeer, text: "hello from bob"})
	got := strings.Join(s.lines, "\n")
	if !strings.Contains(got, "you: hello from alice") || !strings.Contains(got, "peer: hello from bob") {
		t.Fatalf("lines %v", s.lines)
	}
}

func TestQuitKeys(t *testing.T) {
	s := newScreen(nil)
	_, in, _ := reduce(s, key("q"))
	if in.line != "quit" {
		t.Fatalf("quit %q", in.line)
	}
	s.phase = phaseChat
	_, in, _ = reduce(s, key("q"))
	if in.quit || in.line != "" {
		t.Fatalf("chat q %+v", in)
	}
	_, in, _ = reduce(s, key("ctrl+c"))
	if !in.quit {
		t.Fatal("ctrl+c")
	}
}

func TestScrollKeepsLines(t *testing.T) {
	s := newScreen(nil)
	s.height = 8
	s.focus = focusScroll
	for i := 0; i < 20; i++ {
		s = s.addLine("line")
	}
	before := len(s.lines)
	s.follow = true
	next, _, _ := reduce(s, key("k"))
	if len(next.lines) != before {
		t.Fatalf("len %d", len(next.lines))
	}
	if next.follow {
		t.Fatal("still following")
	}
	view := next.View()
	if !strings.Contains(view, "line") {
		t.Fatalf("view %s", view)
	}
}

func TestPhaseClearsHint(t *testing.T) {
	s := newScreen(nil)
	s.hint = "connecting"
	s.phase = phaseConnecting
	s, _, _ = applyNote(s, note{kind: notePhase, phase: phaseChat})
	if !strings.Contains(s.statusText(), "chat") {
		t.Fatalf("status %q", s.statusText())
	}
	s, _, _ = applyNote(s, note{kind: notePhase, phase: phaseLobby})
	if !strings.Contains(s.statusText(), "lobby") {
		t.Fatalf("status %q", s.statusText())
	}
}

func TestDeskSayKeepsPrintlnSpaces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &app{deskCh: make(chan tea.Msg, 2), ctx: ctx}
	a.say("accepted — dialing", "127.0.0.1:9002")
	a.println("connect error:", "too many requests")
	first := (<-a.deskCh).(note)
	second := (<-a.deskCh).(note)
	if first.kind != noteToast || first.level != toastInfo {
		t.Fatalf("say note %#v", first)
	}
	if first.text != "accepted — dialing 127.0.0.1:9002" {
		t.Fatalf("say %q", first.text)
	}
	if second.kind != noteToast || second.level != toastWarn {
		t.Fatalf("println note %#v", second)
	}
	if second.text != "connect error: too many requests" {
		t.Fatalf("println %q", second.text)
	}
}

func TestDiscardWhenTerminal(t *testing.T) {
	var buf bytes.Buffer
	if discardWhenTerminal(&buf, true) != io.Discard {
		t.Fatal("terminal stderr")
	}
	if discardWhenTerminal(&buf, false) != &buf {
		t.Fatal("redirected stderr")
	}
	if discardWhenTerminal(nil, false) != io.Discard {
		t.Fatal("nil stderr")
	}
}

func TestClearInvite(t *testing.T) {
	s := newScreen(nil)
	s, _, _ = applyNote(s, note{kind: noteInvite, key: "alice", id: "req-1"})
	s, _, _ = applyNote(s, note{kind: noteClear, id: "req-1"})
	if s.rows[0].invite != "" {
		t.Fatalf("invite %q", s.rows[0].invite)
	}
}

func TestToastEnqueueExpireAndDismiss(t *testing.T) {
	s := newScreen(nil)
	s, _, cmd := applyNote(s, note{kind: noteToast, level: toastSuccess, text: "connected"})
	if len(s.toasts) != 1 || s.toasts[0].text != "connected" || cmd == nil {
		t.Fatalf("toast %+v cmd %v", s.toasts, cmd)
	}
	s, _, _ = applyNote(s, note{kind: noteToast, level: toastInfo, text: "connected"})
	if len(s.toasts) != 1 {
		t.Fatalf("coalesce %d", len(s.toasts))
	}
	s, _, _ = reduce(s, key("esc"))
	if len(s.toasts) != 0 {
		t.Fatalf("esc dismiss %d", len(s.toasts))
	}
	s, _, _ = applyNote(s, note{kind: noteToast, level: toastWarn, text: "traversal failed"})
	id := s.toasts[0].id
	s, _, _ = reduce(s, toastExpired{id: id})
	if len(s.toasts) != 0 {
		t.Fatalf("expire left %d", len(s.toasts))
	}
}

func TestToastDoesNotEnterTranscript(t *testing.T) {
	s := newScreen(nil)
	before := len(s.lines)
	s, _, _ = applyNote(s, note{kind: noteToast, level: toastInfo, text: "chat ended"})
	if len(s.lines) != before {
		t.Fatalf("lines grew %v", s.lines)
	}
	if len(s.toasts) != 1 {
		t.Fatalf("toasts %d", len(s.toasts))
	}
}

func TestMouseWheelScrollsChat(t *testing.T) {
	s := newScreen(nil)
	s.width = 80
	s.height = 12
	s.focus = focusCompose
	for i := 0; i < 30; i++ {
		s = s.addLine("line")
	}
	s.follow = true
	s = s.stick()
	before := s.top
	_, _, leftW, _, _ := s.geom()
	next, _, _ := reduce(s, tea.MouseMsg{
		X:      leftW + 2,
		Y:      2,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonWheelUp,
	})
	if next.top >= before {
		t.Fatalf("top %d want less than %d", next.top, before)
	}
	if next.focus != focusScroll {
		t.Fatalf("focus %d", next.focus)
	}
	if next.follow {
		t.Fatal("still following")
	}
}

func TestMouseClickSelectsPeer(t *testing.T) {
	s := newScreen(nil)
	s.width = 80
	s.height = 12
	s, _, _ = applyNote(s, note{kind: noteLookup, key: "alice", state: "available", address: "1"})
	s, _, _ = applyNote(s, note{kind: noteLookup, key: "bob", state: "available", address: "2"})
	s.sel = 0
	s.focus = focusCompose
	next, _, _ := reduce(s, tea.MouseMsg{
		X:      1,
		Y:      2,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})
	if next.focus != focusPeers {
		t.Fatalf("focus %d", next.focus)
	}
	if next.sel != 1 {
		t.Fatalf("sel %d", next.sel)
	}
}

func TestMouseClickDismissesToast(t *testing.T) {
	s := newScreen(nil)
	s.width = 80
	s.height = 12
	s, _, _ = applyNote(s, note{kind: noteToast, level: toastInfo, text: "hello"})
	width, _, _, _, _ := s.geom()
	next, _, _ := reduce(s, tea.MouseMsg{
		X:      width - 1,
		Y:      0,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})
	if len(next.toasts) != 0 {
		t.Fatalf("toasts left %+v", next.toasts)
	}
}
