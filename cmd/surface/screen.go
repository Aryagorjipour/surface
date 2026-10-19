package main

import (
	"errors"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
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
	noteToast
)

type toastLevel uint8

const (
	toastInfo toastLevel = iota
	toastSuccess
	toastWarn
	toastError
)

type note struct {
	kind    noteKind
	phase   phase
	text    string
	key     string
	id      string
	state   string
	address string
	level   toastLevel
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

type toast struct {
	id    uint64
	level toastLevel
	text  string
}

type toastExpired struct{ id uint64 }

const (
	toastTTL    = 3 * time.Second
	maxToasts   = 3
	toastMaxW   = 36
	chromeLines = 2
)

type screen struct {
	app      *app
	phase    phase
	focus    focus
	rows     []row
	sel      int
	lines    []string
	follow   bool
	top      int
	compose  string
	cursor   int
	width    int
	height   int
	myKey    string
	hint     string
	toasts   []toast
	toastSeq uint64
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
	next, in, cmd := reduce(s, msg)
	if in.quit {
		if next.app != nil {
			next.app.cancel()
		}
		return next, tea.Quit
	}
	var action tea.Cmd
	if next.app != nil && (in.line != "" || in.accept != "" || in.reject != "") {
		app := next.app
		line, accept, reject := in.line, in.accept, in.reject
		action = func() tea.Msg {
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
	return next, tea.Batch(cmd, action)
}

func (s screen) View() string {
	width, height, leftW, rightW, body := s.geom()
	left := s.peerLines(leftW, body)
	right := s.chatLines(rightW, body)
	rows := make([]string, 0, height)
	for i := 0; i < body; i++ {
		l := stylePaneCell(left[i], leftW, i == 0, s.focus == focusPeers)
		r := stylePaneCell(right[i], rightW, i == 0, s.focus == focusScroll)
		sep := styleSep.Render("│")
		rows = append(rows, l+sep+r)
	}
	rows = append(rows, styleStatus.Width(width).Render(fit(s.statusText(), width)))
	rows = append(rows, styleCompose(s.focus == focusCompose).Width(width).Render(fit(s.composeText(), width)))
	rows = s.overlayToasts(rows, width)
	return strings.Join(rows, "\n")
}

func reduce(s screen, msg tea.Msg) (screen, intent, tea.Cmd) {
	switch m := msg.(type) {
	case tea.WindowSizeMsg:
		s.width = m.Width
		s.height = m.Height
		s = s.stick()
		return s, intent{}, nil
	case note:
		return applyNote(s, m)
	case toastExpired:
		return s.dropToast(m.id), intent{}, nil
	case lineDone:
		if errors.Is(m.err, errStop) {
			return s, intent{quit: true}, nil
		}
		return s, intent{}, nil
	case tea.MouseMsg:
		return reduceMouse(s, m)
	case tea.KeyMsg:
		return reduceKey(s, m)
	default:
		return s, intent{}, nil
	}
}

func applyNote(s screen, n note) (screen, intent, tea.Cmd) {
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
		text := "incoming request from " + n.key + " id " + n.id
		var cmd tea.Cmd
		s, cmd = s.pushToast(toastInfo, text)
		return s, intent{}, cmd
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
	case noteToast:
		text := strings.Trim(n.text, "\n")
		if text == "" {
			return s, intent{}, nil
		}
		var cmd tea.Cmd
		s, cmd = s.pushToast(n.level, text)
		return s, intent{}, cmd
	}
	return s, intent{}, nil
}

func (s screen) pushToast(level toastLevel, text string) (screen, tea.Cmd) {
	text = strings.TrimSpace(text)
	if text == "" {
		return s, nil
	}
	for i := range s.toasts {
		if s.toasts[i].text == text {
			id := s.toasts[i].id
			s.toasts[i].level = level
			return s, toastTick(id)
		}
	}
	s.toastSeq++
	id := s.toastSeq
	s.toasts = append(s.toasts, toast{id: id, level: level, text: text})
	if len(s.toasts) > maxToasts {
		s.toasts = s.toasts[len(s.toasts)-maxToasts:]
	}
	return s, toastTick(id)
}

func toastTick(id uint64) tea.Cmd {
	return tea.Tick(toastTTL, func(time.Time) tea.Msg {
		return toastExpired{id: id}
	})
}

func (s screen) dropToast(id uint64) screen {
	out := s.toasts[:0]
	for _, t := range s.toasts {
		if t.id != id {
			out = append(out, t)
		}
	}
	s.toasts = out
	return s
}

func (s screen) dismissTopToast() screen {
	if len(s.toasts) == 0 {
		return s
	}
	return s.dropToast(s.toasts[0].id)
}

func reduceKey(s screen, k tea.KeyMsg) (screen, intent, tea.Cmd) {
	switch k.String() {
	case "ctrl+c":
		return s, intent{quit: true}, nil
	case "esc":
		if len(s.toasts) > 0 {
			return s.dismissTopToast(), intent{}, nil
		}
	case "tab":
		s.focus = (s.focus + 1) % 3
		s.hint = ""
		return s, intent{}, nil
	case "shift+tab":
		s.focus = (s.focus + 2) % 3
		s.hint = ""
		return s, intent{}, nil
	}
	switch s.focus {
	case focusScroll:
		next, in := reduceScroll(s, k)
		return next, in, nil
	case focusCompose:
		next, in := reduceCompose(s, k)
		return next, in, nil
	default:
		next, in := reducePeers(s, k)
		return next, in, nil
	}
}

func reduceMouse(s screen, m tea.MouseMsg) (screen, intent, tea.Cmd) {
	width, _, leftW, _, body := s.geom()
	if m.Button == tea.MouseButtonWheelUp || m.Button == tea.MouseButtonWheelDown {
		overChat := m.X > leftW
		overPeers := m.X <= leftW
		if overChat || s.focus == focusScroll {
			s.focus = focusScroll
			s.follow = false
			if m.Button == tea.MouseButtonWheelUp && s.top > 0 {
				s.top--
			}
			if m.Button == tea.MouseButtonWheelDown && s.top+1 < len(s.lines) {
				s.top++
			}
			return s, intent{}, nil
		}
		if overPeers {
			s.focus = focusPeers
			if m.Button == tea.MouseButtonWheelUp && s.sel > 0 {
				s.sel--
			}
			if m.Button == tea.MouseButtonWheelDown && s.sel+1 < len(s.rows) {
				s.sel++
			}
		}
		return s, intent{}, nil
	}
	if m.Action != tea.MouseActionPress || m.Button != tea.MouseButtonLeft {
		return s, intent{}, nil
	}
	if hit, ok := s.toastHit(m.X, m.Y, width); ok {
		return s.dropToast(hit), intent{}, nil
	}
	if m.Y == body {
		return s, intent{}, nil
	}
	switch {
	case m.Y >= body+1:
		s.focus = focusCompose
		s.hint = ""
	case m.X <= leftW:
		s.focus = focusPeers
		s.hint = ""
		row := m.Y - 1
		if row >= 0 && row < len(s.rows) {
			s.sel = row
		}
	default:
		s.focus = focusScroll
		s.hint = ""
	}
	return s, intent{}, nil
}

func (s screen) toastHit(x, y, width int) (uint64, bool) {
	if y < 0 || y >= len(s.toasts) {
		return 0, false
	}
	t := s.toasts[y]
	label := toastStyle(t.level).MaxWidth(toastMaxW).Render(toastLabel(t))
	tw := lipgloss.Width(label)
	if tw > width {
		tw = width
	}
	start := width - tw
	if start < 0 {
		start = 0
	}
	if x < start {
		return 0, false
	}
	return t.id, true
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

func (s screen) geom() (width, height, leftW, rightW, body int) {
	width = s.width
	height = s.height
	if width < 20 {
		width = 20
	}
	if height < 6 {
		height = 6
	}
	leftW = 22
	if width < 70 {
		leftW = 16
	}
	rightW = width - leftW - 1
	if rightW < 8 {
		rightW = 8
		leftW = width - rightW - 1
		if leftW < 8 {
			leftW = 8
		}
	}
	body = height - chromeLines
	if body < 1 {
		body = 1
	}
	return width, height, leftW, rightW, body
}

func (s screen) bodyHeight() int {
	_, _, _, _, body := s.geom()
	return body
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
			mark = "▸ "
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
		return "chat  tab panes  enter send  wheel scroll  click focus  /quit leave"
	case phaseConnecting:
		return "connecting"
	default:
		return "lobby  j/k move  enter connect  a/r invite  wheel/click  l type  q quit"
	}
}

func (s screen) composeText() string {
	prompt := "› "
	runes := []rune(s.compose)
	if s.cursor > len(runes) {
		s.cursor = len(runes)
	}
	if s.focus == focusCompose {
		return prompt + string(runes[:s.cursor]) + "█" + string(runes[s.cursor:])
	}
	return prompt + string(runes)
}

func (s screen) overlayToasts(rows []string, width int) []string {
	if len(s.toasts) == 0 || width < 8 {
		return rows
	}
	limit := len(s.toasts)
	if limit > len(rows) {
		limit = len(rows)
	}
	for i := 0; i < limit; i++ {
		label := toastStyle(s.toasts[i].level).MaxWidth(toastMaxW).Render(toastLabel(s.toasts[i]))
		rows[i] = overlayRight(rows[i], label, width)
	}
	return rows
}

func toastLabel(t toast) string {
	prefix := "info"
	switch t.level {
	case toastSuccess:
		prefix = "ok"
	case toastWarn:
		prefix = "warn"
	case toastError:
		prefix = "err"
	}
	return " " + prefix + " · " + t.text + " "
}

func overlayRight(base, overlay string, width int) string {
	ow := lipgloss.Width(overlay)
	if ow >= width {
		return lipgloss.NewStyle().MaxWidth(width).Render(overlay)
	}
	keep := width - ow
	if keep < 0 {
		keep = 0
	}
	trimmed := truncateCells(base, keep)
	pad := keep - lipgloss.Width(trimmed)
	if pad < 0 {
		pad = 0
	}
	return trimmed + strings.Repeat(" ", pad) + overlay
}

func truncateCells(text string, width int) string {
	if width <= 0 {
		return ""
	}
	if lipgloss.Width(text) <= width {
		return text
	}
	runes := []rune(lipgloss.NewStyle().MaxWidth(width).Render(text))
	return string(runes)
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

var (
	colorMuted  = lipgloss.AdaptiveColor{Light: "#5A6570", Dark: "#8B949E"}
	colorAccent = lipgloss.AdaptiveColor{Light: "#0F6B4C", Dark: "#3DDC97"}
	colorFocus  = lipgloss.AdaptiveColor{Light: "#1A1F24", Dark: "#E6EDF3"}
	colorPanel  = lipgloss.AdaptiveColor{Light: "#F3F5F7", Dark: "#161B22"}
	colorInfo   = lipgloss.AdaptiveColor{Light: "#0B4F6C", Dark: "#7EC8E3"}
	colorOk     = lipgloss.AdaptiveColor{Light: "#0F6B4C", Dark: "#3DDC97"}
	colorWarn   = lipgloss.AdaptiveColor{Light: "#8A5A00", Dark: "#E3B341"}
	colorErr    = lipgloss.AdaptiveColor{Light: "#9B1D20", Dark: "#F85149"}

	styleSep = lipgloss.NewStyle().Foreground(colorMuted)
	styleStatus = lipgloss.NewStyle().
			Foreground(colorMuted).
			Background(colorPanel)
	styleTitle = lipgloss.NewStyle().
			Foreground(colorAccent).
			Bold(true)
	styleTitleFocus = lipgloss.NewStyle().
			Foreground(colorFocus).
			Background(colorAccent).
			Bold(true)
	styleComposeIdle = lipgloss.NewStyle().
				Foreground(colorMuted)
	styleComposeFocus = lipgloss.NewStyle().
				Foreground(colorFocus).
				Bold(true)
)

func stylePaneCell(text string, width int, title, focused bool) string {
	fitted := fit(text, width)
	if title {
		st := styleTitle
		if focused {
			st = styleTitleFocus
		}
		return st.Width(width).Render(fitted)
	}
	trimmed := strings.TrimSpace(text)
	switch {
	case strings.HasPrefix(text, "▸ "):
		return lipgloss.NewStyle().Foreground(colorAccent).Width(width).Render(fitted)
	case strings.HasPrefix(trimmed, "you:"):
		return lipgloss.NewStyle().Foreground(colorAccent).Width(width).Render(fitted)
	case strings.HasPrefix(trimmed, "peer:"):
		return lipgloss.NewStyle().Foreground(colorInfo).Width(width).Render(fitted)
	default:
		return lipgloss.NewStyle().Width(width).Render(fitted)
	}
}

func styleCompose(focused bool) lipgloss.Style {
	if focused {
		return styleComposeFocus
	}
	return styleComposeIdle
}

func toastStyle(level toastLevel) lipgloss.Style {
	fg := colorInfo
	switch level {
	case toastSuccess:
		fg = colorOk
	case toastWarn:
		fg = colorWarn
	case toastError:
		fg = colorErr
	}
	return lipgloss.NewStyle().
		Foreground(lipgloss.AdaptiveColor{Light: "#F7FAFC", Dark: "#0D1117"}).
		Background(fg).
		Bold(true)
}
