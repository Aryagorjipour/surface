package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Aryagorjipour/surface/internal/session"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/mattn/go-isatty"
)

func stdinTerminal(r io.Reader) bool {
	f, ok := r.(*os.File)
	if !ok {
		return false
	}
	return isatty.IsTerminal(f.Fd())
}

func discardWhenTerminal(stderr io.Writer, stderrIsTerminal bool) io.Writer {
	if stderrIsTerminal || stderr == nil {
		return io.Discard
	}
	return stderr
}

func (a *app) note(m tea.Msg) {
	if a == nil || a.deskCh == nil {
		return
	}
	select {
	case a.deskCh <- m:
	case <-a.ctx.Done():
	}
}

func (a *app) markPhase(p phase) {
	a.note(note{kind: notePhase, phase: p})
}

func lineText(args ...any) string {
	return strings.TrimSuffix(fmt.Sprintln(args...), "\n")
}

func (a *app) println(args ...any) {
	if a != nil && a.deskCh != nil {
		a.note(note{kind: noteToast, level: toastWarn, text: lineText(args...)})
		return
	}
	fmt.Println(args...)
}

func (a *app) showEnded() {
	a.toast(toastInfo, "chat ended")
	a.markPhase(phaseLobby)
}

func (a *app) toast(level toastLevel, args ...any) {
	if a != nil && a.deskCh != nil {
		a.note(note{kind: noteToast, level: level, text: lineText(args...)})
		return
	}
	fmt.Println(args...)
}

func (a *app) serveDesk(in io.Reader, out io.Writer) error {
	s := newScreen(a)
	opts := []tea.ProgramOption{tea.WithAltScreen(), tea.WithMouseCellMotion()}
	if f, ok := in.(*os.File); ok {
		opts = append(opts, tea.WithInput(f))
	}
	if f, ok := out.(*os.File); ok {
		opts = append(opts, tea.WithOutput(f))
	}
	prog := tea.NewProgram(s, opts...)
	go func() {
		for msg := range a.deskCh {
			prog.Send(msg)
		}
	}()
	_, err := prog.Run()
	return err
}

func (a *app) acceptRequest(id string) error {
	if id == "" {
		a.println("usage: accept <request-id>")
		return errors.New("usage")
	}
	proofText, err := a.postAction(id, "accept")
	if err != nil {
		a.println("accept error:", err)
		return err
	}
	raw, err := session.DecodeProof(proofText)
	if err != nil {
		a.println("accept error:", err)
		return err
	}
	from := a.callerFor(id)
	a.gate.remember(id, from, a.myKey, raw)
	a.say("accepted — waiting for peer to dial")
	a.note(note{kind: noteClear, id: id})
	go func() {
		_ = a.attempt(false, "", id, from, a.myKey, raw)
	}()
	return nil
}

func (a *app) rejectRequest(id string) error {
	if id == "" {
		a.println("usage: reject <request-id>")
		return errors.New("usage")
	}
	if _, err := a.postAction(id, "reject"); err != nil {
		a.println("reject error:", err)
		return err
	}
	a.say("rejected")
	a.note(note{kind: noteClear, id: id})
	return nil
}
