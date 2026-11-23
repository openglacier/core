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

use serde_json::{json, Value};

use super::{complete::Completer, execute_background_line, local_command, screen::{Buffer, Rect, Screen, Style, WORKSPACE_OFF, WORKSPACE_ON}, term::{self, text_width, Event, Key, Keys, Line, Raw}, Client, Config, Local, Output, Result, NAME};
use std::io::Write;

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

        let (width, height) = term::size()?;
        let mut frame = Buffer::new(width, height);
        app.draw(&mut frame);
        terminal.screen.draw(frame)?;
        if app.exit {
            break;
        }

        match terminal.keys.next(EVENT_POLL)? {
            Some(Event::Key(key)) => app.handle_key(key),
            Some(Event::Paste(text)) => app.line.insert(&text),
            None => {}
        }
    }

    Ok(if app.failed { ExitCode::FAILURE } else { ExitCode::SUCCESS })
}

/// The alternate screen in raw mode while it lives (the mode is restored after the screen, field order).
struct TerminalSession {
    screen: Screen,
    keys: Keys,
    _raw: Raw,
}

impl TerminalSession {
    fn enter() -> Result<Self> {
        let raw = Raw::enter()?;
        let mut screen = Screen::new(io::stdout());
        write!(screen.out(), "{}", WORKSPACE_ON)?;
        Ok(Self { screen, keys: Keys::default(), _raw: raw })
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        let _ = write!(self.screen.out(), "{}", WORKSPACE_OFF);
        let _ = self.screen.out().flush();
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
    /// Table output is drawn unwrapped and scrolls sideways instead.
    wrap: bool,
    hscroll: u16,
}

impl Job {
    fn new(id: u64, command: String, wrap: bool) -> Self {
        Self {
            id,
            command,
            status: JobStatus::Running,
            lines: VecDeque::new(),
            run_id: None,
            scroll: 0,
            follow: true,
            wrap,
            hscroll: 0,
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

    /// The lines to show: the output, or a placeholder before any.
    fn shown_lines(&self) -> Vec<&str> {
        if self.lines.is_empty() {
            return vec!["waiting for output…"];
        }
        self.lines.iter().map(String::as_str).collect()
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
    completer: Completer,
    events: mpsc::Sender<JobEvent>,
    jobs: Vec<Job>,
    focused: Option<usize>,
    next_job_id: u64,
    line: Line,
    view: ViewMode,
    notice: String,
    exit: bool,
    failed: bool,
}

impl App {
    fn new(config: Config, events: mpsc::Sender<JobEvent>) -> Self {
        Self {
            completer: Completer::new(config.clone()),
            config,
            events,
            jobs: Vec::new(),
            focused: None,
            next_job_id: 1,
            line: Line::default(),
            view: ViewMode::Tiles,
            notice: "Enter: run · Alt-Enter: run in focused job · Ctrl-O: edit · Ctrl-R: rerun · ↑/↓: history · Tab: complete/focus · Ctrl-W: close · :help".into(),
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

    fn handle_key(&mut self, key: Key) {
        match key {
            Key::Ctrl('c') => self.exit = true,
            Key::Ctrl('t') => { self.view = self.view.toggled(); self.notice = format!("layout: {}", self.view.label()); }
            Key::Ctrl('w') => self.close_focused(),
            Key::Ctrl('x') => self.cancel_focused(),
            Key::Ctrl('o') => self.edit_focused(),
            Key::Ctrl('r') => self.rerun_focused(),
            Key::Alt(character @ '1'..='9') => { let index = (character as usize) - ('1' as usize); if index < self.jobs.len() { self.focused = Some(index); } }
            Key::AltEnter => self.submit_input(true),
            Key::Enter => self.submit_input(false),
            Key::Tab if !self.line.text.trim().is_empty() => self.complete(),
            Key::Tab => self.focus_next(),
            Key::BackTab => self.focus_previous(),
            Key::PageUp => self.scroll_focused_up(10),
            Key::PageDown => self.scroll_focused_down(10),
            Key::ShiftUp => self.scroll_focused_up(1),
            Key::ShiftDown => self.scroll_focused_down(1),
            Key::Left if self.line.text.is_empty() => self.scroll_focused_sideways(-8),
            Key::Right if self.line.text.is_empty() => self.scroll_focused_sideways(8),
            key => { self.line.edit(key); }
        }
    }

    /// Runs the input line; `in_place` reuses the focused job's view.
    fn submit_input(&mut self, in_place: bool) {
        let line = self.line.submit();
        if line.is_empty() {
            return;
        }
        if line.starts_with(':') {
            return self.handle_tui_command(&line);
        }
        match local_command(&line) {
            Ok(None) => self.spawn_job(line, in_place),
            Ok(Some(Local::Quit)) => self.exit = true,
            Ok(Some(Local::Help)) => self.handle_tui_command(":help"),
            Ok(Some(Local::Format(output))) => {
                self.config.output = output;
                self.notice = format!("format: {}", output.label());
            }
            Ok(Some(Local::Reconnect)) => {
                self.completer.invalidate();
                self.notice = match Client::connect(&self.config) {
                    Ok(_) => format!("connection to {} verified", self.config.address),
                    Err(error) => format!("{NAME}: {error}"),
                };
            }
            Err(error) => self.notice = format!("{NAME}: {error}"),
        }
    }

    /// Completes the word before the cursor: the single candidate, or the
    /// prefix every candidate shares, with the candidates in the notice line.
    fn complete(&mut self) {
        self.notice = match &self.line.complete(&self.completer)[..] { [] => "no completion".into(), [_] => String::new(), many => truncate(&many.join("  "), 400) };
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
            ":edit" => self.edit_focused(),
            ":rerun" => self.rerun_focused(),
            ":clear" => {
                if let Some(job) = self.focused_job_mut() {
                    job.lines.clear();
                    job.scroll = 0;
                }
            }
            ":help" => {
                self.notice = "TUI: :tabs :tiles :next :prev :close :cancel :clear :edit :rerun :quit · Alt-Enter run in focused job · Ctrl-O edit · Ctrl-R rerun · ↑/↓ history · Shift-↑/↓ scroll · format table|pretty|compact · import FILE [COLLECTION] · Tab complete · ←/→ scroll a table · Ctrl-T toggle · Ctrl-W close · Ctrl-X cancel LLM · Alt-1..9 focus".into();
            }
            _ => self.notice = format!("unknown TUI command: {line}"),
        }
    }

    /// Starts `line` in a new job, or in the focused one when `in_place`: that
    /// job takes a new id, so output still coming from its previous run is dropped.
    fn spawn_job(&mut self, line: String, in_place: bool) {
        let job_id = self.next_job_id;
        self.next_job_id = self.next_job_id.wrapping_add(1);
        let config = self.config.clone();
        let events = self.events.clone();
        let job = Job::new(job_id, line.clone(), self.config.output != Output::Table);
        match self.focused.filter(|_| in_place) {
            Some(index) => {
                let previous = std::mem::replace(&mut self.jobs[index], job);
                self.notice = if matches!(previous.status, JobStatus::Running | JobStatus::Queued) {
                    format!("job #{job_id} replaces #{}; its remote request continues unless cancelled", previous.id)
                } else {
                    format!("job #{job_id} started in place of #{}", previous.id)
                };
            }
            None => {
                self.jobs.push(job);
                self.focused = Some(self.jobs.len() - 1);
                self.notice = format!("job #{job_id} started");
            }
        }

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

    /// Puts the focused job's line in the input, to change it and run it again.
    fn edit_focused(&mut self) {
        match self.focused.and_then(|index| self.jobs.get(index)) {
            Some(job) => {
                self.line.set(job.command.clone());
                self.notice = "Enter: new job · Alt-Enter: run in this job".into();
            }
            None => self.notice = "no focused job".into(),
        }
    }

    fn rerun_focused(&mut self) {
        match self.focused.and_then(|index| self.jobs.get(index)) {
            Some(job) => {
                let line = job.command.clone();
                self.spawn_job(line, true);
            }
            None => self.notice = "no focused job".into(),
        }
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
        self.spawn_job(command, false);
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

    fn scroll_focused_sideways(&mut self, amount: i32) {
        if let Some(job) = self.focused_job_mut().filter(|job| !job.wrap) {
            job.hscroll = (i32::from(job.hscroll) + amount).clamp(0, i32::from(u16::MAX)) as u16;
        }
    }

    fn draw(&self, frame: &mut Buffer) {
        let (tops, jobs, bottoms) = frame.area().rows(&[1], &[1, 3]);
        self.draw_tabs(frame, tops[0]);
        self.draw_jobs(frame, jobs);
        frame.line(bottoms[0], &[(self.notice.as_str(), Style::PLAIN)]);
        self.draw_input(frame, bottoms[1]);
    }

    fn draw_tabs(&self, frame: &mut Buffer, area: Rect) {
        let title = format!(" openglacier · {} ", self.view.label());
        let labels: Vec<(String, Style)> = self.jobs.iter().enumerate().map(|(index, job)| {
            let label = format!(" {}{}:{} ", job.status.glyph(), job.id, truncate(&job.command, 18));
            (label, if self.focused == Some(index) { Style::SELECTED } else { Style::PLAIN })
        }).collect();
        let mut pieces = vec![(title.as_str(), Style::BOLD)];
        if labels.is_empty() {
            pieces.push((" no jobs", Style::PLAIN));
        }
        pieces.extend(labels.iter().map(|(label, style)| (label.as_str(), *style)));
        frame.line(area, &pieces);
    }

    fn draw_jobs(&self, frame: &mut Buffer, area: Rect) {
        if self.jobs.is_empty() {
            frame.block(area, "workspace", Style::PLAIN);
            frame.paragraph(area.inner(), ["No jobs yet. Type a query or .OPERATION {JSON} below and press Enter.", "", "The TUI uses the same concurrent per-job connections as the regular REPL."], true, Some(0), 0);
            return;
        }

        let indexes = visible_job_indexes(self.jobs.len(), self.focused.unwrap_or(0), self.view);
        let rects = tile_rects(area, indexes.len());
        for (rect, index) in rects.into_iter().zip(indexes) {
            self.draw_job(frame, rect, index);
        }
    }

    fn draw_job(&self, frame: &mut Buffer, area: Rect, index: usize) {
        let job = &self.jobs[index];
        let style = if self.focused == Some(index) { Style::BOLD } else { Style::PLAIN };
        frame.block(area, &job.title(), style);
        // Following the output: its last rows, wrapped or not.
        let top = (!job.follow && job.scroll != u16::MAX).then_some(usize::from(job.scroll));
        frame.paragraph(area.inner(), job.shown_lines(), job.wrap, top, usize::from(job.hscroll));
    }

    fn draw_input(&self, frame: &mut Buffer, area: Rect) {
        let cursor_columns = text_width(&self.line.text[..self.line.cursor]);
        let content_width = usize::from(area.width.saturating_sub(2));
        let horizontal_scroll = cursor_columns.saturating_sub(content_width.saturating_sub(1));
        frame.block(area, "", Style::PLAIN);
        frame.paragraph(area.inner(), [self.line.text.as_str()], false, Some(0), horizontal_scroll);
        let visible_cursor = cursor_columns.saturating_sub(horizontal_scroll).min(u16::MAX as usize) as u16;
        let max_x = area.x.saturating_add(area.width.saturating_sub(2));
        let cursor_x = area.x.saturating_add(1).saturating_add(visible_cursor).min(max_x);
        frame.set_cursor(cursor_x, area.y.saturating_add(1));
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
        2 => area.columns().to_vec(),
        _ => {
            let mut rects: Vec<Rect> = area.halves().into_iter().flat_map(Rect::columns).collect();
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
    #[test] fn edits_reruns_in_place_and_browses_history() { let (mut config, _) = super::super::Args::parse(Vec::<String>::new()).unwrap(); (config.address, config.identity) = ("127.0.0.1:1".into(), None); let mut app = App::new(config, mpsc::channel().0); for line in ["on a", "on b"] { app.line.insert(line); app.submit_input(false); } assert_eq!(app.jobs.len(), 2); app.edit_focused(); assert_eq!(app.line.text, "on b"); app.line.insert(" | limit 1"); app.submit_input(true); assert_eq!((app.jobs.len(), app.jobs[1].command.as_str(), app.jobs[1].id), (2, "on b | limit 1", 3)); app.rerun_focused(); assert_eq!((app.jobs.len(), app.jobs[1].id), (2, 4)); app.line.back(); assert_eq!(app.line.text, "on b | limit 1"); app.line.back(); app.line.back(); app.line.back(); assert_eq!(app.line.text, "on a"); app.line.forward(); assert_eq!(app.line.text, "on b"); app.line.insert("x"); app.line.forward(); assert_eq!(app.line.text, "on bx"); }
    #[test] fn tiles_follow_groups_of_four() { assert_eq!(visible_job_indexes(7, 0, ViewMode::Tiles), vec![0, 1, 2, 3]); assert_eq!(visible_job_indexes(7, 5, ViewMode::Tiles), vec![4, 5, 6]); assert_eq!(visible_job_indexes(7, 5, ViewMode::Tabs), vec![5]); }
}
