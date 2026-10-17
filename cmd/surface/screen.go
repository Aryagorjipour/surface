package main

import (
	"errors"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

type phase uint8

const (
	phaseLobby phase = iota
	phaseConnecting
	phaseChat
)

type focus uint8

const (
	focusPeers focus = iota
	focusScroll
	focusCompose
)

type noteKind uint8

const (
	noteText noteKind = iota
	notePeer
	noteSelf
	noteLookup
	noteInvite
	notePhase
	noteClear
)

type note struct {
	kind    noteKind
	phase   phase
	text    string
	key     string
	id      string
	state   string
	address string
}

type row struct {
	key     string
	state   string
	address string
	invite  string
}

type intent struct {
	line   string
	accept string
	reject string
	quit   bool
}

type lineDone struct{ err error }

type screen struct {
	app     *app
	phase   phase
	focus   focus
	rows    []row
	sel     int
	lines   []string
	follow  bool
	top     int
	compose string
	cursor  int
	width   int
	height  int
	myKey   string
	hint    string
}

func newScreen(a *app) screen {
	key := ""
	if a != nil {
		key = a.myKey
	}
	return screen{
		app:    a,
		follow: true,
		width:  80,
		height: 24,
		myKey:  key,
		lines: []string{
			"commands: lookup <key> | connect <key> | accept <id> | reject <id> | quit",
			"(in chat: just type messages; /quit leaves chat)",
		},
	}
}

func (s screen) Init() tea.Cmd { return nil }

func (s screen) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	next, in := reduce(s, msg)
	if in.quit {
		if next.app != nil {
			next.app.cancel()
		}
		return next, tea.Quit
	}
	if next.app == nil || (in.line == "" && in.accept == "" && in.reject == "") {
		return next, nil
	}
	app := next.app
	line, accept, reject := in.line, in.accept, in.reject
	return next, func() tea.Msg {
		if accept != "" {
			_ = app.acceptRequest(accept)
		}
		if reject != "" {
			_ = app.rejectRequest(reject)
		}
		if line == "" {
			return nil
		}
		return lineDone{err: app.handleLine(line)}
	}
}

func (s screen) View() string {
	width := s.width
	height := s.height
	if width < 20 {
		width = 20
	}
	if height < 6 {
		height = 6
	}
	leftW := 20
	if width < 70 {
		leftW = 16
	}
	rightW := width - leftW - 1
	body := height - 2
	left := s.peerLines(leftW, body)
	right := s.chatLines(rightW, body)
	var b strings.Builder
	for i := 0; i < body; i++ {
		b.WriteString(fit(left[i], leftW))
		b.WriteByte('|')
		b.WriteString(fit(right[i], rightW))
		if i+1 < body {
			b.WriteByte('\n')
		}
	}
	b.WriteByte('\n')
	b.WriteString(fit(s.statusText(), width))
	b.WriteByte('\n')
	b.WriteString(fit(s.composeText(), width))
	return b.String()
}

func reduce(s screen, msg tea.Msg) (screen, intent) {
	switch m := msg.(type) {
	case tea.WindowSizeMsg:
		s.width = m.Width
		s.height = m.Height
		s = s.stick()
		return s, intent{}
	case note:
		return applyNote(s, m), intent{}
	case lineDone:
		if errors.Is(m.err, errStop) {
			return s, intent{quit: true}
		}
		return s, intent{}
	case tea.KeyMsg:
		return reduceKey(s, m)
	default:
		return s, intent{}
	}
}

func applyNote(s screen, n note) screen {
	switch n.kind {
	case noteText:
		text := strings.Trim(n.text, "\n")
		if text != "" {
			s = s.addLine(text)
		}
	case notePeer:
		s = s.addLine("peer: " + n.text)
	case noteSelf:
		s = s.addLine("you: " + n.text)
	case noteLookup:
		s = s.touch(n.key, n.state, n.address, "")
		if n.text != "" {
			s = s.addLine(n.text)
		}
	case noteInvite:
		s = s.touch(n.key, "", "", n.id)
		s = s.addLine("incoming request from " + n.key + " id " + n.id)
	case notePhase:
		s.phase = n.phase
		s.hint = ""
		if s.phase == phaseChat {
			s.focus = focusCompose
		}
		if s.phase == phaseLobby && s.compose == "" {
			s.focus = focusPeers
		}
	case noteClear:
		for i := range s.rows {
			if s.rows[i].invite == n.id {
				s.rows[i].invite = ""
			}
		}
	}
	return s
}

func reduceKey(s screen, k tea.KeyMsg) (screen, intent) {
	switch k.String() {
	case "ctrl+c":
		return s, intent{quit: true}
	case "tab":
		s.focus = (s.focus + 1) % 3
		s.hint = ""
		return s, intent{}
	case "shift+tab":
		s.focus = (s.focus + 2) % 3
		s.hint = ""
		return s, intent{}
	}
	switch s.focus {
	case focusScroll:
		return reduceScroll(s, k)
	case focusCompose:
		return reduceCompose(s, k)
	default:
		return reducePeers(s, k)
	}
}

func reducePeers(s screen, k tea.KeyMsg) (screen, intent) {
	switch k.String() {
	case "j", "down":
		if s.sel+1 < len(s.rows) {
			s.sel++
		}
	case "k", "up":
		if s.sel > 0 {
			s.sel--
		}
	case "enter":
		row, ok := s.selected()
		if ok && row.key != "" && s.phase != phaseChat {
			s.phase = phaseConnecting
			s.hint = "connecting"
			return s, intent{line: "connect " + row.key}
		}
		if s.phase == phaseChat {
			s.hint = "type /quit to leave chat"
		}
	case "a":
		row, ok := s.selected()
		if ok && row.invite != "" {
			return s, intent{accept: row.invite}
		}
	case "r":
		row, ok := s.selected()
		if ok && row.invite != "" {
			return s, intent{reject: row.invite}
		}
	case "q":
		if s.phase == phaseChat {
			s.hint = "type /quit to leave chat"
			return s, intent{}
		}
		return s, intent{line: "quit"}
	case "l":
		s.focus = focusCompose
	case "g":
		s.top = 0
		s.follow = false
	case "G":
		s.follow = true
		s = s.stick()
	}
	return s, intent{}
}

func reduceScroll(s screen, k tea.KeyMsg) (screen, intent) {
	body := s.bodyHeight()
	switch k.String() {
	case "j", "down":
		s.follow = false
		if s.top+1 < len(s.lines) {
			s.top++
		}
	case "k", "up":
		s.follow = false
		if s.top > 0 {
			s.top--
		}
	case "pgdown":
		s.follow = false
		s.top += body
		s = s.clampTop()
	case "pgup":
		s.follow = false
		s.top -= body
		if s.top < 0 {
			s.top = 0
		}
	case "g":
		s.top = 0
		s.follow = false
	case "G":
		s.follow = true
		s = s.stick()
	}
	return s, intent{}
}

func reduceCompose(s screen, k tea.KeyMsg) (screen, intent) {
	switch k.String() {
	case "enter":
		line := strings.TrimSpace(s.compose)
		s.compose = ""
		s.cursor = 0
		if line == "" {
			return s, intent{}
		}
		if s.phase != phaseChat && strings.HasPrefix(line, "connect ") {
			s.phase = phaseConnecting
			s.hint = "connecting"
		}
		return s, intent{line: line}
	case "esc":
		s.compose = ""
		s.cursor = 0
		s.focus = focusPeers
	case "backspace":
		s = s.deleteBack()
	case "left":
		if s.cursor > 0 {
			s.cursor--
		}
	case "right":
		if s.cursor < len([]rune(s.compose)) {
			s.cursor++
		}
	case " ":
		s = s.insert(" ")
	default:
		if k.Type == tea.KeyRunes && !k.Paste {
			s = s.insert(string(k.Runes))
		}
	}
	return s, intent{}
}

func (s screen) selected() (row, bool) {
	if s.sel < 0 || s.sel >= len(s.rows) {
		return row{}, false
	}
	return s.rows[s.sel], true
}

func (s screen) touch(key, state, address, invite string) screen {
	if key == "" {
		return s
	}
	for i := range s.rows {
		if s.rows[i].key != key {
			continue
		}
		if state != "" {
			s.rows[i].state = state
		}
		if address != "" {
			s.rows[i].address = address
		}
		if invite != "" {
			s.rows[i].invite = invite
		}
		s.sel = i
		return s
	}
	if state == "" {
		state = "request"
	}
	s.rows = append(s.rows, row{key: key, state: state, address: address, invite: invite})
	s.sel = len(s.rows) - 1
	return s
}

func (s screen) addLine(text string) screen {
	s.lines = append(s.lines, text)
	return s.stick()
}

func (s screen) stick() screen {
	if !s.follow {
		return s.clampTop()
	}
	body := s.bodyHeight()
	s.top = len(s.lines) - body
	if s.top < 0 {
		s.top = 0
	}
	return s
}

func (s screen) clampTop() screen {
	maxTop := len(s.lines) - 1
	if maxTop < 0 {
		maxTop = 0
	}
	if s.top > maxTop {
		s.top = maxTop
	}
	if s.top < 0 {
		s.top = 0
	}
	return s
}

func (s screen) bodyHeight() int {
	h := s.height - 2
	if h < 1 {
		return 1
	}
	return h
}

func (s screen) insert(text string) screen {
	runes := []rune(s.compose)
	if s.cursor > len(runes) {
		s.cursor = len(runes)
	}
	head := string(runes[:s.cursor])
	tail := string(runes[s.cursor:])
	s.compose = head + text + tail
	s.cursor += len([]rune(text))
	return s
}

func (s screen) deleteBack() screen {
	runes := []rune(s.compose)
	if s.cursor == 0 || len(runes) == 0 {
		return s
	}
	if s.cursor > len(runes) {
		s.cursor = len(runes)
	}
	s.compose = string(runes[:s.cursor-1]) + string(runes[s.cursor:])
	s.cursor--
	return s
}

func (s screen) peerLines(width, n int) []string {
	out := make([]string, n)
	out[0] = fit("peers", width)
	for i := 1; i < n; i++ {
		idx := i - 1
		if idx >= len(s.rows) {
			out[i] = ""
			continue
		}
		row := s.rows[idx]
		mark := "  "
		if idx == s.sel && s.focus == focusPeers {
			mark = "> "
		}
		state := row.state
		if row.invite != "" {
			state = "request"
		}
		out[i] = mark + row.key + " " + state
	}
	return out
}

func (s screen) chatLines(width, n int) []string {
	out := make([]string, n)
	title := "chat"
	if s.myKey != "" {
		title = s.myKey
	}
	out[0] = fit(title, width)
	start := s.top
	for i := 1; i < n; i++ {
		idx := start + i - 1
		if idx >= 0 && idx < len(s.lines) {
			out[i] = s.lines[idx]
		}
	}
	return out
}

func (s screen) statusText() string {
	if s.hint != "" {
		return s.hint
	}
	switch s.phase {
	case phaseChat:
		return "chat  tab switches pane  enter sends  /quit leaves"
	case phaseConnecting:
		return "connecting"
	default:
		return "lobby  j/k move  enter connect  a accept  r reject  l type  q quit"
	}
}

func (s screen) composeText() string {
	prompt := "> "
	if s.focus == focusCompose {
		prompt = "> "
	}
	runes := []rune(s.compose)
	if s.cursor > len(runes) {
		s.cursor = len(runes)
	}
	return prompt + string(runes[:s.cursor]) + string(runes[s.cursor:])
}

func fit(text string, width int) string {
	if width <= 0 {
		return ""
	}
	runes := []rune(text)
	if len(runes) > width {
		return string(runes[:width])
	}
	return text + strings.Repeat(" ", width-len(runes))
}
