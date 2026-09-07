//! Optional terminal workspace for concurrent ogcli jobs.
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::{
    collections::VecDeque,
    io::{self, IsTerminal},
    process::ExitCode,
    sync::mpsc,
    thread,
    time::Duration,
};

use crossterm::{
    cursor::Show,
    event::{self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
    Frame, Terminal,
};
use serde_json::{json, Value};

use super::{execute_background_line, Client, Config, Result, NAME};

const EVENT_POLL: Duration = Duration::from_millis(50);
const MAX_BUFFER_LINES: usize = 2_000;
const MAX_VISIBLE_TILES: usize = 4;

pub(super) fn run(mut config: Config) -> Result<ExitCode> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "--tui requires an interactive terminal").into());
    }

    config.prepare_for_background()?;
    // Authenticate once before taking ownership of the terminal so password
    // prompts never race the raw-mode input loop.
    let client = Client::connect(&config)?;
    drop(client);

    let mut terminal = TerminalSession::enter()?;
    let (events_tx, events_rx) = mpsc::channel::<JobEvent>();
    let mut app = App::new(config, events_tx);

    loop {
        while let Ok(job_event) = events_rx.try_recv() {
            app.apply_job_event(job_event);
        }

        terminal.terminal.draw(|frame| app.draw(frame))?;
        if app.exit {
            break;
        }

        if event::poll(EVENT_POLL)? {
            match event::read()? {
                Event::Key(key) => app.handle_key(key),
                Event::Paste(text) => app.insert_str(&text),
                Event::Resize(_, _) => {}
                _ => {}
            }
        }
    }

    Ok(if app.failed { ExitCode::FAILURE } else { ExitCode::SUCCESS })
}

struct TerminalSession {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
}

impl TerminalSession {
    fn enter() -> Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        if let Err(error) = execute!(stdout, EnterAlternateScreen, EnableBracketedPaste) {
            let _ = disable_raw_mode();
            return Err(error.into());
        }
        let backend = CrosstermBackend::new(stdout);
        match Terminal::new(backend) {
            Ok(terminal) => Ok(Self { terminal }),
            Err(error) => {
                let _ = disable_raw_mode();
                let _ = execute!(io::stdout(), DisableBracketedPaste, LeaveAlternateScreen, Show);
                Err(error.into())
            }
        }
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), DisableBracketedPaste, LeaveAlternateScreen, Show);
        let _ = self.terminal.show_cursor();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ViewMode {
    Tiles,
    Tabs,
}

impl ViewMode {
    const fn label(self) -> &'static str {
        match self {
            Self::Tiles => "tiles",
            Self::Tabs => "tabs",
        }
    }

    const fn toggled(self) -> Self {
        match self {
            Self::Tiles => Self::Tabs,
            Self::Tabs => Self::Tiles,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JobStatus {
    Running,
    Queued,
    Done,
    Cancelled,
    Failed,
}

impl JobStatus {
    const fn label(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Queued => "queued",
            Self::Done => "done",
            Self::Cancelled => "cancelled",
            Self::Failed => "error",
        }
    }

    const fn glyph(self) -> &'static str {
        match self {
            Self::Running => "▶",
            Self::Queued => "…",
            Self::Done => "✓",
            Self::Cancelled => "×",
            Self::Failed => "!",
        }
    }
}

#[derive(Debug)]
struct Job {
    id: u64,
    command: String,
    status: JobStatus,
    lines: VecDeque<String>,
    run_id: Option<u64>,
    scroll: u16,
    follow: bool,
}

impl Job {
    fn new(id: u64, command: String) -> Self {
        Self {
            id,
            command,
            status: JobStatus::Running,
            lines: VecDeque::new(),
            run_id: None,
            scroll: 0,
            follow: true,
        }
    }

    fn push(&mut self, line: String) {
        if let Some((run_id, state)) = parse_run_metadata(&line) {
            self.run_id = Some(run_id);
            if state.as_deref() == Some("queued") {
                self.status = JobStatus::Queued;
            } else if state.as_deref() == Some("running") {
                self.status = JobStatus::Running;
            }
        }
        if let Ok(value) = serde_json::from_str::<Value>(&line) {
            if self.status == JobStatus::Queued
                && value.get("type").and_then(Value::as_str) == Some("token")
            {
                self.status = JobStatus::Running;
            }
            if value
                .get("statistics")
                .and_then(|statistics| statistics.get("finishReason"))
                .and_then(Value::as_str)
                == Some("cancelled")
            {
                self.status = JobStatus::Cancelled;
            }
        }
        for part in line.lines() {
            self.lines.push_back(part.to_owned());
        }
        if self.lines.len() > MAX_BUFFER_LINES {
            let overflow = self.lines.len() - MAX_BUFFER_LINES;
            self.lines.drain(..overflow);
        }
        if self.follow {
            self.scroll = u16::MAX;
        }
    }

    fn title(&self) -> String {
        let command = truncate(&self.command, 36);
        match self.run_id {
            Some(run_id) => format!("{} [{}] #{}/run:{} {}", self.status.glyph(), self.status.label(), self.id, run_id, command),
            None => format!("{} [{}] #{} {}", self.status.glyph(), self.status.label(), self.id, command),
        }
    }

    fn text(&self) -> Text<'static> {
        if self.lines.is_empty() {
            return Text::from(Line::from("waiting for output…"));
        }
        Text::from(
            self.lines
                .iter()
                .cloned()
                .map(Line::from)
                .collect::<Vec<_>>(),
        )
    }

    fn scroll_up(&mut self, amount: u16) {
        let current = if self.scroll == u16::MAX {
            self.lines.len().saturating_sub(1).min(u16::MAX as usize) as u16
        } else {
            self.scroll
        };
        self.scroll = current.saturating_sub(amount);
        self.follow = false;
    }

    fn scroll_down(&mut self, amount: u16) {
        if self.follow {
            return;
        }
        let max = self.lines.len().saturating_sub(1).min(u16::MAX as usize) as u16;
        self.scroll = self.scroll.saturating_add(amount).min(max);
        if self.scroll >= max {
            self.scroll = u16::MAX;
            self.follow = true;
        }
    }
}

#[derive(Debug)]
enum JobEvent {
    Output { job_id: u64, message: String },
    Finished { job_id: u64, success: bool },
}

struct App {
    config: Config,
    events: mpsc::Sender<JobEvent>,
    jobs: Vec<Job>,
    focused: Option<usize>,
    next_job_id: u64,
    input: String,
    cursor: usize,
    view: ViewMode,
    notice: String,
    exit: bool,
    failed: bool,
}

impl App {
    fn new(config: Config, events: mpsc::Sender<JobEvent>) -> Self {
        Self {
            config,
            events,
            jobs: Vec::new(),
            focused: None,
            next_job_id: 1,
            input: String::new(),
            cursor: 0,
            view: ViewMode::Tiles,
            notice: "Enter: run · Tab: focus · Ctrl-T: tabs/tiles · Ctrl-W: close · Ctrl-X/:cancel: cancel LLM · PgUp/PgDn: scroll · :help".into(),
            exit: false,
            failed: false,
        }
    }

    fn apply_job_event(&mut self, event: JobEvent) {
        match event {
            JobEvent::Output { job_id, message } => {
                if let Some(job) = self.jobs.iter_mut().find(|job| job.id == job_id) {
                    job.push(message);
                }
            }
            JobEvent::Finished { job_id, success } => {
                if let Some(job) = self.jobs.iter_mut().find(|job| job.id == job_id) {
                    if !matches!(job.status, JobStatus::Cancelled) {
                        job.status = if success { JobStatus::Done } else { JobStatus::Failed };
                    }
                    if !success {
                        self.failed = true;
                    }
                }
            }
        }
    }

    fn handle_key(&mut self, key: KeyEvent) {
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            match key.code {
                KeyCode::Char('c') => {
                    self.exit = true;
                    return;
                }
                KeyCode::Char('t') => {
                    self.view = self.view.toggled();
                    self.notice = format!("layout: {}", self.view.label());
                    return;
                }
                KeyCode::Char('w') => {
                    self.close_focused();
                    return;
                }
                KeyCode::Char('x') => {
                    self.cancel_focused();
                    return;
                }
                KeyCode::Char('a') => {
                    self.cursor = 0;
                    return;
                }
                KeyCode::Char('e') => {
                    self.cursor = self.input.len();
                    return;
                }
                _ => {}
            }
        }

        if key.modifiers.contains(KeyModifiers::ALT) {
            if let KeyCode::Char(character @ '1'..='9') = key.code {
                let index = (character as usize) - ('1' as usize);
                if index < self.jobs.len() {
                    self.focused = Some(index);
                }
                return;
            }
        }

        match key.code {
            KeyCode::Enter => self.submit_input(),
            KeyCode::Tab => self.focus_next(),
            KeyCode::BackTab => self.focus_previous(),
            KeyCode::PageUp => self.scroll_focused_up(10),
            KeyCode::PageDown => self.scroll_focused_down(10),
            KeyCode::Up if self.input.is_empty() => self.scroll_focused_up(1),
            KeyCode::Down if self.input.is_empty() => self.scroll_focused_down(1),
            KeyCode::Left => self.move_cursor_left(),
            KeyCode::Right => self.move_cursor_right(),
            KeyCode::Home => self.cursor = 0,
            KeyCode::End => self.cursor = self.input.len(),
            KeyCode::Backspace => self.backspace(),
            KeyCode::Delete => self.delete(),
            KeyCode::Esc => {
                self.input.clear();
                self.cursor = 0;
            }
            KeyCode::Char(character) if !key.modifiers.contains(KeyModifiers::CONTROL) => self.insert(character),
            _ => {}
        }
    }

    fn submit_input(&mut self) {
        let line = self.input.trim().to_owned();
        self.input.clear();
        self.cursor = 0;
        if line.is_empty() {
            return;
        }
        if line.starts_with(':') {
            self.handle_tui_command(&line);
        } else {
            self.spawn_job(line);
        }
    }

    fn handle_tui_command(&mut self, line: &str) {
        match line {
            ":q" | ":quit" | ":exit" => self.exit = true,
            ":tabs" => {
                self.view = ViewMode::Tabs;
                self.notice = "layout: tabs".into();
            }
            ":tiles" => {
                self.view = ViewMode::Tiles;
                self.notice = "layout: tiles".into();
            }
            ":next" => self.focus_next(),
            ":prev" => self.focus_previous(),
            ":close" => self.close_focused(),
            ":cancel" => self.cancel_focused(),
            ":clear" => {
                if let Some(job) = self.focused_job_mut() {
                    job.lines.clear();
                    job.scroll = 0;
                }
            }
            ":help" => {
                self.notice = "TUI: :tabs :tiles :next :prev :close :cancel :clear :quit · Ctrl-T toggle · Ctrl-W close · Ctrl-X cancel LLM · Alt-1..9 focus".into();
            }
            _ => self.notice = format!("unknown TUI command: {line}"),
        }
    }

    fn spawn_job(&mut self, line: String) {
        let job_id = self.next_job_id;
        self.next_job_id = self.next_job_id.wrapping_add(1);
        let config = self.config.clone();
        let events = self.events.clone();
        self.jobs.push(Job::new(job_id, line.clone()));
        self.focused = Some(self.jobs.len() - 1);
        self.notice = format!("job #{job_id} started");

        thread::spawn(move || {
            let result = execute_background_line(&config, &line, |message| {
                events
                    .send(JobEvent::Output { job_id, message })
                    .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "TUI event channel closed").into())
            });
            match result {
                Ok(code) => {
                    let _ = events.send(JobEvent::Finished { job_id, success: code == ExitCode::SUCCESS });
                }
                Err(error) => {
                    let _ = events.send(JobEvent::Output { job_id, message: format!("{NAME}: {error}") });
                    let _ = events.send(JobEvent::Finished { job_id, success: false });
                }
            }
        });
    }

    fn cancel_focused(&mut self) {
        let Some(index) = self.focused else {
            self.notice = "no focused job".into();
            return;
        };
        if !self.jobs[index].command.trim_start().starts_with(".llm.generate") {
            self.notice = "focused job has no cancellation operation".into();
            return;
        }
        let Some(run_id) = self.jobs[index].run_id else {
            self.notice = "focused LLM job has no runId yet".into();
            return;
        };
        let command = format!(".llm.cancel {}", json!({"runId": run_id}));
        self.notice = format!("cancelling run {run_id}");
        self.spawn_job(command);
    }

    fn close_focused(&mut self) {
        let Some(index) = self.focused else {
            return;
        };
        let was_active = matches!(self.jobs[index].status, JobStatus::Running | JobStatus::Queued);
        self.jobs.remove(index);
        if was_active {
            self.notice = "view closed; the remote request continues unless cancelled".into();
        }
        self.focused = if self.jobs.is_empty() {
            None
        } else {
            Some(index.min(self.jobs.len() - 1))
        };
    }

    fn focus_next(&mut self) {
        if self.jobs.is_empty() {
            self.focused = None;
            return;
        }
        self.focused = Some(match self.focused {
            Some(index) => (index + 1) % self.jobs.len(),
            None => 0,
        });
    }

    fn focus_previous(&mut self) {
        if self.jobs.is_empty() {
            self.focused = None;
            return;
        }
        self.focused = Some(match self.focused {
            Some(0) | None => self.jobs.len() - 1,
            Some(index) => index - 1,
        });
    }

    fn focused_job_mut(&mut self) -> Option<&mut Job> {
        self.focused.and_then(|index| self.jobs.get_mut(index))
    }

    fn scroll_focused_up(&mut self, amount: u16) {
        if let Some(job) = self.focused_job_mut() {
            job.scroll_up(amount);
        }
    }

    fn scroll_focused_down(&mut self, amount: u16) {
        if let Some(job) = self.focused_job_mut() {
            job.scroll_down(amount);
        }
    }

    fn insert(&mut self, character: char) {
        self.input.insert(self.cursor, character);
        self.cursor += character.len_utf8();
    }

    fn insert_str(&mut self, value: &str) {
        self.input.insert_str(self.cursor, value);
        self.cursor += value.len();
    }

    fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let previous = self.input[..self.cursor]
            .char_indices()
            .next_back()
            .map(|(index, _)| index)
            .unwrap_or(0);
        self.input.drain(previous..self.cursor);
        self.cursor = previous;
    }

    fn delete(&mut self) {
        if self.cursor >= self.input.len() {
            return;
        }
        let next = self.input[self.cursor..]
            .char_indices()
            .nth(1)
            .map(|(index, _)| self.cursor + index)
            .unwrap_or(self.input.len());
        self.input.drain(self.cursor..next);
    }

    fn move_cursor_left(&mut self) {
        if self.cursor == 0 {
            return;
        }
        self.cursor = self.input[..self.cursor]
            .char_indices()
            .next_back()
            .map(|(index, _)| index)
            .unwrap_or(0);
    }

    fn move_cursor_right(&mut self) {
        if self.cursor >= self.input.len() {
            return;
        }
        self.cursor = self.input[self.cursor..]
            .char_indices()
            .nth(1)
            .map(|(index, _)| self.cursor + index)
            .unwrap_or(self.input.len());
    }

    fn draw(&self, frame: &mut Frame<'_>) {
        let area = frame.area();
        let sections = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Min(3),
                Constraint::Length(1),
                Constraint::Length(3),
            ])
            .split(area);

        self.draw_tabs(frame, sections[0]);
        self.draw_jobs(frame, sections[1]);
        frame.render_widget(Paragraph::new(self.notice.as_str()), sections[2]);
        self.draw_input(frame, sections[3]);
    }

    fn draw_tabs(&self, frame: &mut Frame<'_>, area: Rect) {
        let mut spans = vec![Span::styled(
            format!(" openglacier · {} ", self.view.label()),
            Style::default().add_modifier(Modifier::BOLD),
        )];
        if self.jobs.is_empty() {
            spans.push(Span::raw(" no jobs"));
        } else {
            for (index, job) in self.jobs.iter().enumerate() {
                let label = format!(" {}{}:{} ", job.status.glyph(), job.id, truncate(&job.command, 18));
                let style = if self.focused == Some(index) {
                    Style::default().add_modifier(Modifier::BOLD | Modifier::REVERSED)
                } else {
                    Style::default()
                };
                spans.push(Span::styled(label, style));
            }
        }
        frame.render_widget(Paragraph::new(Line::from(spans)), area);
    }

    fn draw_jobs(&self, frame: &mut Frame<'_>, area: Rect) {
        if self.jobs.is_empty() {
            let welcome = Paragraph::new(
                "No jobs yet. Type a query or .OPERATION {JSON} below and press Enter.\n\nThe TUI uses the same concurrent per-job connections as the regular REPL.",
            )
            .block(Block::default().borders(Borders::ALL).title("workspace"))
            .wrap(Wrap { trim: false });
            frame.render_widget(welcome, area);
            return;
        }

        let indexes = visible_job_indexes(self.jobs.len(), self.focused.unwrap_or(0), self.view);
        let rects = tile_rects(area, indexes.len());
        for (rect, index) in rects.into_iter().zip(indexes) {
            self.draw_job(frame, rect, index);
        }
    }

    fn draw_job(&self, frame: &mut Frame<'_>, area: Rect, index: usize) {
        let job = &self.jobs[index];
        let focused = self.focused == Some(index);
        let border_style = if focused {
            Style::default().add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        let visible_height = area.height.saturating_sub(2) as usize;
        let scroll = if job.follow || job.scroll == u16::MAX {
            job.lines.len().saturating_sub(visible_height).min(u16::MAX as usize) as u16
        } else {
            job.scroll
        };
        let paragraph = Paragraph::new(job.text())
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(border_style)
                    .title(job.title()),
            )
            .scroll((scroll, 0))
            .wrap(Wrap { trim: false });
        frame.render_widget(paragraph, area);
    }

    fn draw_input(&self, frame: &mut Frame<'_>, area: Rect) {
        let cursor_chars = self.input[..self.cursor].chars().count();
        let content_width = area.width.saturating_sub(2) as usize;
        let horizontal_scroll = cursor_chars
            .saturating_sub(content_width.saturating_sub(1))
            .min(u16::MAX as usize) as u16;
        let input = Paragraph::new(self.input.as_str())
            .block(Block::default().borders(Borders::ALL).title(""))
            .scroll((0, horizontal_scroll));
        frame.render_widget(input, area);
        let visible_cursor = cursor_chars.saturating_sub(horizontal_scroll as usize).min(u16::MAX as usize) as u16;
        let max_x = area.x.saturating_add(area.width.saturating_sub(2));
        let cursor_x = area.x.saturating_add(1).saturating_add(visible_cursor).min(max_x);
        frame.set_cursor_position((cursor_x, area.y.saturating_add(1)));
    }
}

fn visible_job_indexes(job_count: usize, focused: usize, view: ViewMode) -> Vec<usize> {
    if job_count == 0 {
        return Vec::new();
    }
    if view == ViewMode::Tabs {
        return vec![focused.min(job_count - 1)];
    }
    if job_count <= MAX_VISIBLE_TILES {
        return (0..job_count).collect();
    }
    let start = (focused / MAX_VISIBLE_TILES) * MAX_VISIBLE_TILES;
    (start..(start + MAX_VISIBLE_TILES).min(job_count)).collect()
}

fn tile_rects(area: Rect, count: usize) -> Vec<Rect> {
    match count {
        0 => Vec::new(),
        1 => vec![area],
        2 => Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(area)
            .to_vec(),
        _ => {
            let rows = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(area);
            let mut rects = Vec::with_capacity(4);
            for row in rows.iter().take(2) {
                rects.extend(
                    Layout::default()
                        .direction(Direction::Horizontal)
                        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                        .split(*row)
                        .iter()
                        .copied(),
                );
            }
            rects.truncate(count);
            rects
        }
    }
}

fn parse_run_metadata(line: &str) -> Option<(u64, Option<String>)> {
    let value: Value = serde_json::from_str(line).ok()?;
    let run_id = value.get("runId").and_then(Value::as_u64)?;
    let state = value.get("state").and_then(Value::as_str).map(str::to_owned);
    Some((run_id, state))
}

fn truncate(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let prefix: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn run_metadata_is_extracted() { assert_eq!( parse_run_metadata(r#"{"runId":7,"state":"queued","type":"run"}"#), Some((7, Some("queued".to_owned()))) ); assert_eq!(parse_run_metadata(r#"{"text":"x"}"#), None); }

    #[test] fn tiles_follow_groups_of_four() { assert_eq!(visible_job_indexes(7, 0, ViewMode::Tiles), vec![0, 1, 2, 3]); assert_eq!(visible_job_indexes(7, 5, ViewMode::Tiles), vec![4, 5, 6]); assert_eq!(visible_job_indexes(7, 5, ViewMode::Tabs), vec![5]); }
}
