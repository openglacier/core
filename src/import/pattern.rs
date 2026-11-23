//! The regular expressions of the import, copied from the Python worker: a backtracking matcher for the subset they use.
//!
//! Syntax: `|`, `(…)`, `(?:…)`, `(?P<name>…)`, `(?i)`, `? * + {m} {m,} {m,n}` (greedy), `^ $ \b .`, classes `[…]`/`[^…]`
//! with ranges, `\d \s \w` and their negations, `\u{…} \t \n` and escaped punctuation. Semantics are those of the regex
//! crate it replaces: leftmost-first, `$` at the very end, `.` all but `\n`, Unicode `\s \w \b`, simple case folding.
//! The search runs without memo while short, then again with a visited set of (instruction, position): linear at worst.
#![cfg_attr(rustfmt, rustfmt_skip)]

#[derive(Clone, Debug)]
enum Item { Char(char), Range(char, char), Digit(bool), Space(bool), Word(bool) }

#[derive(Clone, Debug)]
enum Inst { Char(char, bool), Class(Vec<Item>, bool, bool), Any, Split(usize, usize), Jmp(usize), Save(usize), Start, End, Boundary, Match }

/// The instructions run: a character test is an index in the ASCII tables and in the full tests.
#[derive(Clone, Copy, Debug)]
enum Op { Test(u32), Split(u32, u32), Jmp(u32), Save(u32), Start, End, Boundary, Match }

#[derive(Clone, Debug)]
enum Node { Inst(Inst), Group(Option<usize>, Box<Node>), Concat(Vec<Node>), Alt(Vec<Node>), Repeat(Box<Node>, u32, Option<u32>) }

/// A compiled pattern.
#[derive(Clone, Debug)]
pub(crate) struct Pattern { ops: Vec<Op>, ascii: Vec<u128>, tests: Vec<Inst>, first: Option<u128>, anchored: bool, span: (usize, Option<usize>), nfa: Option<Box<Nfa>>, names: Vec<(String, usize)>, slots: usize }

/// The groups of a match, by number (0 is the whole match) or by name.
#[derive(Debug)]
pub(crate) struct Captures<'t> { text: &'t str, slots: Vec<Option<usize>>, names: &'t [(String, usize)] }

macro_rules! pattern { ($pattern:expr) => {{ static RE: std::sync::LazyLock<$crate::import::pattern::Pattern> = std::sync::LazyLock::new(|| $crate::import::pattern::Pattern::new($pattern).expect("valid pattern")); &*RE }} }
pub(crate) use pattern;

fn is_word(c: char) -> bool { c.is_alphanumeric() || c == '_' || matches!(c, '\u{300}'..='\u{36f}' | '\u{1ab0}'..='\u{1aff}' | '\u{1dc0}'..='\u{1dff}' | '\u{20d0}'..='\u{20ff}' | '\u{fe20}'..='\u{fe2f}' | '\u{200c}' | '\u{200d}') }
fn lower(c: char) -> char { let mut it = c.to_lowercase(); match (it.next(), it.next()) { (Some(l), None) => l, _ => c } }
fn upper(c: char) -> char { let mut it = c.to_uppercase(); match (it.next(), it.next()) { (Some(u), None) => u, _ => c } }

impl Item {
    fn matches(&self, c: char) -> bool {
        match *self { Item::Char(x) => c == x, Item::Range(a, b) => (a..=b).contains(&c), Item::Digit(yes) => c.is_numeric() == yes, Item::Space(yes) => c.is_whitespace() == yes, Item::Word(yes) => is_word(c) == yes }
    }
}

impl Inst {
    fn accepts(&self, c: char) -> bool {
        match self {
            Inst::Char(x, fold) => c == *x || *fold && (lower(c) == *x || upper(c) == upper(*x)),
            Inst::Class(items, negated, fold) => { let hit = |c| items.iter().any(|item| item.matches(c)); (hit(c) || *fold && (hit(lower(c)) || hit(upper(c)))) != *negated }
            Inst::Any => c != '\n',
            _ => false,
        }
    }
}

struct Parser<'p> { chars: std::iter::Peekable<std::str::Chars<'p>>, fold: bool, groups: usize, names: Vec<(String, usize)> }

impl Parser<'_> {
    fn alt(&mut self) -> Result<Node, String> {
        let mut branches = vec![self.concat()?];
        while self.chars.next_if_eq(&'|').is_some() { branches.push(self.concat()?); }
        Ok(if branches.len() == 1 { branches.pop().unwrap_or(Node::Concat(vec![])) } else { Node::Alt(branches) })
    }

    fn concat(&mut self) -> Result<Node, String> {
        let mut nodes = Vec::new();
        while let Some(&c) = self.chars.peek() {
            if c == '|' || c == ')' { break; }
            self.chars.next();
            let atom = match c {
                '(' => match self.group()? { Some(node) => node, None => continue },
                '.' => Node::Inst(Inst::Any), '^' => Node::Inst(Inst::Start), '$' => Node::Inst(Inst::End),
                '[' => Node::Inst(self.class()?),
                '\\' => match self.escape()? { Ok(item) => Node::Inst(self.item(item)), Err(inst) => Node::Inst(inst) },
                '?' | '*' | '+' | '{' => return Err(format!("nothing to repeat before {c}")),
                c => Node::Inst(self.item(Item::Char(c))),
            };
            nodes.push(self.repeat(atom)?);
        }
        Ok(if nodes.len() == 1 { nodes.pop().unwrap_or(Node::Concat(vec![])) } else { Node::Concat(nodes) })
    }

    /// After `(`: a group, or `None` for a flag group `(?i)`.
    fn group(&mut self) -> Result<Option<Node>, String> {
        let index = if self.chars.next_if_eq(&'?').is_some() {
            match self.chars.next() {
                Some(':') => None,
                Some('i') if self.chars.next_if_eq(&')').is_some() => { self.fold = true; return Ok(None); }
                Some('P') if self.chars.next_if_eq(&'<').is_some() => {
                    let name: String = self.chars.by_ref().take_while(|c| *c != '>').collect();
                    self.groups += 1; self.names.push((name, self.groups)); Some(self.groups)
                }
                other => return Err(format!("unsupported group (?{other:?}")),
            }
        } else { self.groups += 1; Some(self.groups) };
        let inner = self.alt()?;
        if self.chars.next() != Some(')') { return Err("unclosed group".into()); }
        Ok(Some(Node::Group(index, Box::new(inner))))
    }

    fn repeat(&mut self, mut atom: Node) -> Result<Node, String> {
        while let Some(&c) = self.chars.peek() {
            let (min, max) = match c {
                '?' => (0, Some(1)), '*' => (0, None), '+' => (1, None),
                '{' => {
                    self.chars.next();
                    let spec: String = self.chars.by_ref().take_while(|c| *c != '}').collect();
                    let number = |text: &str| text.trim().parse::<u32>().map_err(|_| format!("bad repetition {{{spec}}}"));
                    let bounds = match spec.split_once(',') { None => { let n = number(&spec)?; (n, Some(n)) } Some((min, "")) => (number(min)?, None), Some((min, max)) => (number(min)?, Some(number(max)?)) };
                    atom = Node::Repeat(Box::new(atom), bounds.0, bounds.1);
                    continue;
                }
                _ => break,
            };
            self.chars.next();
            if self.chars.peek() == Some(&'?') { return Err("lazy repetition unsupported".into()); }
            atom = Node::Repeat(Box::new(atom), min, max);
        }
        Ok(atom)
    }

    /// After `\`: a class item, or an assertion.
    fn escape(&mut self) -> Result<Result<Item, Inst>, String> {
        Ok(Ok(match self.chars.next().ok_or("trailing backslash")? {
            'd' => Item::Digit(true), 'D' => Item::Digit(false), 's' => Item::Space(true), 'S' => Item::Space(false), 'w' => Item::Word(true), 'W' => Item::Word(false),
            'b' => return Ok(Err(Inst::Boundary)), 't' => Item::Char('\t'), 'n' => Item::Char('\n'), 'r' => Item::Char('\r'),
            'u' if self.chars.next_if_eq(&'{').is_some() => {
                let hex: String = self.chars.by_ref().take_while(|c| *c != '}').collect();
                Item::Char(u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32).ok_or("bad \\u{…}")?)
            }
            c if !c.is_alphanumeric() => Item::Char(c),
            c => return Err(format!("unsupported escape \\{c}")),
        }))
    }

    fn class(&mut self) -> Result<Inst, String> {
        let negated = self.chars.next_if_eq(&'^').is_some();
        let mut items = Vec::new();
        loop {
            let item = match self.chars.next().ok_or("unclosed class")? {
                ']' if !items.is_empty() => break,
                '\\' => self.escape()?.map_err(|_| "assertion in class")?,
                c => Item::Char(c),
            };
            match item {
                Item::Char(from) if self.chars.peek() == Some(&'-') => {
                    self.chars.next();
                    match self.chars.next().ok_or("unclosed class")? {
                        ']' => { items.extend([Item::Char(from), Item::Char('-')]); break; }
                        '\\' => match self.escape()? { Ok(Item::Char(to)) => items.push(Item::Range(from, to)), _ => return Err("bad class range".into()) },
                        to => items.push(Item::Range(from, to)),
                    }
                }
                item => items.push(item),
            }
        }
        Ok(Inst::Class(items, negated, self.fold))
    }

    fn item(&self, item: Item) -> Inst {
        match item { Item::Char(c) => Inst::Char(if self.fold { lower(c) } else { c }, self.fold), item => Inst::Class(vec![item], false, self.fold) }
    }
}

/// Whether every match starts with `^`: then only the start of the text is tried.
fn anchored(node: &Node) -> bool {
    match node { Node::Inst(inst) => matches!(inst, Inst::Start), Node::Group(_, inner) => anchored(inner), Node::Concat(nodes) => nodes.first().is_some_and(anchored), Node::Alt(branches) => branches.iter().all(anchored), Node::Repeat(inner, min, _) => *min > 0 && anchored(inner) }
}

/// An anchored pattern without `\b` of 128 instructions at most, run as a set of states (one bit per instruction):
/// `bytes[b]` are the tests accepting the ASCII byte `b`, `closure[at start + 2 × at end][pc]` the tests and `Match`
/// reached from `pc` without reading.
#[derive(Clone, Debug)]
struct Nfa { bytes: Vec<u128>, closure: [Vec<u128>; 4], matched: u128 }

impl Nfa {
    fn new(ops: &[Op], ascii: &[u128]) -> Option<Self> {
        if ops.len() > 128 || ops.iter().any(|op| matches!(op, Op::Boundary)) { return None; }
        let bytes = (0..128).map(|b| ops.iter().enumerate().filter(|(_, op)| matches!(op, Op::Test(test) if ascii[*test as usize] >> b & 1 == 1)).fold(0_u128, |set, (pc, _)| set | 1 << pc)).collect();
        let closure = |start: bool, end: bool| (0..ops.len()).map(|from| {
            let (mut set, mut todo, mut seen) = (0_u128, vec![from], 0_u128);
            while let Some(pc) = todo.pop() {
                if seen >> pc & 1 == 1 { continue; }
                seen |= 1 << pc;
                match ops[pc] { Op::Test(_) | Op::Match => set |= 1 << pc, Op::Split(a, b) => todo.extend([a as usize, b as usize]), Op::Jmp(a) => todo.push(a as usize), Op::Save(_) => todo.push(pc + 1), Op::Start if start => todo.push(pc + 1), Op::End if end => todo.push(pc + 1), _ => {} }
            }
            set
        }).collect();
        let matched = ops.iter().position(|op| matches!(op, Op::Match)).map_or(0, |pc| 1 << pc);
        Some(Self { bytes, closure: [closure(false, false), closure(true, false), closure(false, true), closure(true, true)], matched })
    }

    fn is_match(&self, pattern: &Pattern, text: &str) -> bool {
        let mut states = self.closure[1 + 2 * usize::from(text.is_empty())][0];
        let mut pos = 0;
        loop {
            if states & self.matched != 0 { return true; }
            let Some(&b) = text.as_bytes().get(pos) else { return false };
            let mut hits = if b < 128 { pos += 1; states & self.bytes[b as usize] } else {
                let c = text[pos..].chars().next().unwrap_or_default(); pos += c.len_utf8();
                (0..128).filter(|pc| states >> pc & 1 == 1 && matches!(pattern.ops[*pc], Op::Test(test) if pattern.tests[test as usize].accepts(c))).fold(0, |set, pc| set | 1 << pc)
            };
            let closure = &self.closure[2 * usize::from(pos == text.len())];
            states = 0;
            while hits != 0 { states |= closure[hits.trailing_zeros() as usize + 1]; hits &= hits - 1; }
            if states == 0 { return false; }
        }
    }
}

/// The bytes a match spans at least and, when it must end with `$`, at most (a test of a non-ASCII character counts 4).
fn span(node: &Node) -> (usize, Option<usize>) {
    match node {
        Node::Inst(Inst::Start | Inst::End | Inst::Boundary) => (0, Some(0)),
        Node::Inst(Inst::Char(c, false)) => (1, Some(c.len_utf8())),
        Node::Inst(Inst::Class(items, false, false)) if items.iter().all(|item| matches!(item, Item::Char(c) | Item::Range(_, c) if c.is_ascii())) => (1, Some(1)),
        Node::Inst(_) => (1, Some(4)),
        Node::Group(_, inner) => span(inner),
        Node::Concat(nodes) => nodes.iter().map(span).fold((0, Some(0)), |(min, max), (a, b)| (min + a, max.zip(b).map(|(max, b)| max + b))),
        Node::Alt(branches) => branches.iter().map(span).reduce(|(min, max), (a, b)| (min.min(a), max.zip(b).map(|(max, b)| max.max(b)))).unwrap_or((0, Some(0))),
        Node::Repeat(inner, low, high) => { let (min, max) = span(inner); (min * *low as usize, max.zip(*high).map(|(max, high)| max * high as usize)) }
    }
}

/// Whether every match ends with `$`.
fn ended(node: &Node) -> bool {
    match node { Node::Inst(inst) => matches!(inst, Inst::End), Node::Group(_, inner) => ended(inner), Node::Concat(nodes) => nodes.last().is_some_and(ended), Node::Alt(branches) => branches.iter().all(ended), Node::Repeat(..) => false }
}

/// The ASCII bytes a match can start with, or `None` when it can start otherwise (empty, at the end).
fn first_bytes(ops: &[Op], ascii: &[u128]) -> Option<u128> {
    let (mut set, mut todo, mut seen) = (0_u128, vec![0], vec![false; ops.len()]);
    while let Some(pc) = todo.pop() {
        if std::mem::replace(&mut seen[pc], true) { continue; }
        match ops[pc] {
            Op::Test(test) => set |= ascii[test as usize],
            Op::Split(a, b) => todo.extend([a as usize, b as usize]),
            Op::Jmp(a) => todo.push(a as usize),
            Op::Save(_) | Op::Start | Op::Boundary => todo.push(pc + 1),
            _ => return None,
        }
    }
    Some(set)
}

fn compile(node: &Node, out: &mut Vec<Inst>) {
    match node {
        Node::Inst(inst) => out.push(inst.clone()),
        Node::Group(index, inner) => {
            if let Some(index) = index { out.push(Inst::Save(2 * index)); }
            compile(inner, out);
            if let Some(index) = index { out.push(Inst::Save(2 * index + 1)); }
        }
        Node::Concat(nodes) => nodes.iter().for_each(|node| compile(node, out)),
        Node::Alt(branches) => {
            let mut jumps = Vec::new();
            for (i, branch) in branches.iter().enumerate() {
                let split = out.len();
                if i + 1 < branches.len() { out.push(Inst::Split(split + 1, 0)); }
                compile(branch, out);
                if i + 1 < branches.len() { jumps.push(out.len()); out.push(Inst::Jmp(0)); out[split] = Inst::Split(split + 1, out.len()); }
            }
            let end = out.len();
            for jump in jumps { out[jump] = Inst::Jmp(end); }
        }
        Node::Repeat(inner, min, max) => {
            for _ in 0..*min { compile(inner, out); }
            match max {
                None => { let split = out.len(); out.push(Inst::Split(split + 1, 0)); compile(inner, out); out.push(Inst::Jmp(split)); out[split] = Inst::Split(split + 1, out.len()); }
                Some(max) => {
                    let splits: Vec<usize> = (*min..*max).map(|_| { let split = out.len(); out.push(Inst::Split(split + 1, 0)); compile(inner, out); split }).collect();
                    let end = out.len();
                    for split in splits { out[split] = Inst::Split(split + 1, end); }
                }
            }
        }
    }
}

/// A branch to explore (`pc`, position), or a group bound to restore (slot with the top bit, value + 1 or 0).
#[derive(Clone, Copy, Debug)]
struct Step(u32, u32);
const RESTORE: u32 = 1 << 31;

impl Pattern {
    pub(crate) fn new(pattern: &str) -> Result<Self, String> {
        let mut parser = Parser { chars: pattern.chars().peekable(), fold: false, groups: 0, names: Vec::new() };
        let node = parser.alt()?;
        if parser.chars.next().is_some() { return Err("unbalanced )".into()); }
        let mut insts = vec![Inst::Save(0)];
        compile(&node, &mut insts);
        insts.extend([Inst::Save(1), Inst::Match]);
        // Character tests answer ASCII from a 128-bit table, the rest through the instruction itself.
        let (mut ascii, mut tests) = (Vec::new(), Vec::new());
        let ops: Vec<Op> = insts.into_iter().map(|inst| match inst {
            Inst::Split(a, b) => Op::Split(a as u32, b as u32), Inst::Jmp(a) => Op::Jmp(a as u32), Inst::Save(slot) => Op::Save(slot as u32),
            Inst::Start => Op::Start, Inst::End => Op::End, Inst::Boundary => Op::Boundary, Inst::Match => Op::Match,
            test => { ascii.push((0..128_u8).filter(|b| test.accepts(char::from(*b))).fold(0_u128, |set, b| set | 1 << b)); tests.push(test); Op::Test(tests.len() as u32 - 1) }
        }).collect();
        let first = first_bytes(&ops, &ascii);
        let span = span(&node);
        let span = (span.0, span.1.filter(|_| anchored(&node) && ended(&node)));
        let nfa = if anchored(&node) { Nfa::new(&ops, &ascii).map(Box::new) } else { None };
        Ok(Self { nfa, ops, ascii, tests, first, span, anchored: anchored(&node), names: parser.names, slots: 2 * parser.groups + 2 })
    }

    pub(crate) fn is_match(&self, text: &str) -> bool { self.search(text, false).is_some() }

    pub(crate) fn captures<'t>(&'t self, text: &'t str) -> Option<Captures<'t>> {
        self.search(text, true).map(|slots| Captures { text, slots, names: &self.names })
    }

    /// The leftmost-first match: without memo under a step budget, else with the visited set.
    fn search(&self, text: &str, groups: bool) -> Option<Vec<Option<usize>>> {
        // A text whose first byte (anchored) or every byte (else) is ASCII outside the first bytes cannot match.
        if let Some(first) = self.first {
            let can_start = |b: &u8| *b >= 128 || first >> b & 1 == 1;
            if if self.anchored { !text.as_bytes().first().is_some_and(can_start) } else { !text.bytes().any(|b| can_start(&b)) } { return None; }
        }
        if text.len() >= RESTORE as usize || text.len() < self.span.0 || self.span.1.is_some_and(|max| text.len() > max) { return None; }
        if let Some(nfa) = &self.nfa {
            if !nfa.is_match(self, text) { return None; }
            if !groups { return Some(Vec::new()); }
        }
        thread_local! { static SCRATCH: std::cell::Cell<(Vec<Step>, Vec<Option<usize>>)> = const { std::cell::Cell::new((Vec::new(), Vec::new())) }; }
        let (mut stack, mut slots) = SCRATCH.take();
        slots.clear(); slots.resize(if groups { self.slots } else { 0 }, None);
        let budget = 64 * (text.len() + 1) * if self.anchored { 1 } else { 4 };
        let found = self.backtrack::<false>(text, groups, &mut [], budget, &mut stack, &mut slots).or_else(|| {
            let mut visited = vec![0_u64; (self.ops.len() * (text.len() + 1)).div_ceil(64)];
            stack.clear(); slots.iter_mut().for_each(|slot| *slot = None);
            self.backtrack::<true>(text, groups, &mut visited, usize::MAX, &mut stack, &mut slots)
        });
        let out = found.filter(|found| *found).map(|_| slots.clone());
        SCRATCH.set((stack, slots));
        out
    }

    /// Whether the text matches, `None` when the budget runs out; `slots` hold the groups of the match when asked.
    fn backtrack<const MEMO: bool>(&self, text: &str, groups: bool, visited: &mut [u64], mut budget: usize, stack: &mut Vec<Step>, slots: &mut [Option<usize>]) -> Option<bool> {
        let bytes = text.as_bytes();
        stack.clear();
        for start in (0..=text.len()).filter(|i| text.is_char_boundary(*i)) {
            if self.anchored && start > 0 { break; }
            stack.push(Step(0, start as u32));
            while let Some(Step(pc, pos)) = stack.pop() {
                if pc & RESTORE != 0 { slots[(pc & !RESTORE) as usize] = pos.checked_sub(1).map(|pos| pos as usize); continue; }
                let (mut pc, mut pos) = (pc as usize, pos as usize);
                loop {
                    if MEMO {
                        let bit = pc * (text.len() + 1) + pos;
                        if visited[bit / 64] & (1 << (bit % 64)) != 0 { break; }
                        visited[bit / 64] |= 1 << (bit % 64);
                    } else { budget = budget.checked_sub(1)?; }
                    match self.ops[pc] {
                        Op::Test(test) => match bytes.get(pos) {
                            Some(&b) if b < 128 => if self.ascii[test as usize] >> b & 1 == 1 { pc += 1; pos += 1 } else { break },
                            Some(_) => match text[pos..].chars().next() { Some(c) if self.tests[test as usize].accepts(c) => { pc += 1; pos += c.len_utf8(); } _ => break },
                            None => break,
                        },
                        Op::Split(first, second) => { stack.push(Step(second, pos as u32)); pc = first as usize; }
                        Op::Jmp(target) => pc = target as usize,
                        Op::Save(_) if !groups => pc += 1,
                        Op::Save(slot) => { stack.push(Step(slot | RESTORE, slots[slot as usize].map_or(0, |pos| pos as u32 + 1))); slots[slot as usize] = Some(pos); pc += 1; }
                        Op::Start => if pos == 0 { pc += 1 } else { break },
                        Op::End => if pos == text.len() { pc += 1 } else { break },
                        Op::Boundary => {
                            let before = text[..pos].chars().next_back().is_some_and(is_word);
                            if before != text[pos..].chars().next().is_some_and(is_word) { pc += 1 } else { break }
                        }
                        Op::Match => return Some(true),
                    }
                }
            }
        }
        Some(false)
    }
}

/// One group of a match.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Match<'t>(&'t str);

impl<'t> Match<'t> {
    pub(crate) fn as_str(&self) -> &'t str { self.0 }
    pub(crate) fn len(&self) -> usize { self.0.len() }
}

impl<'t> Captures<'t> {
    pub(crate) fn get(&self, index: usize) -> Option<Match<'t>> {
        match (self.slots.get(2 * index).copied().flatten(), self.slots.get(2 * index + 1).copied().flatten()) { (Some(start), Some(end)) => Some(Match(&self.text[start..end])), _ => None }
    }
    pub(crate) fn name(&self, name: &str) -> Option<Match<'t>> { self.names.iter().find(|(n, _)| n == name).and_then(|(_, index)| self.get(*index)) }
}

impl std::ops::Index<usize> for Captures<'_> {
    type Output = str;
    fn index(&self, index: usize) -> &str { self.get(index).map_or_else(|| panic!("no group {index}"), |found| found.0) }
}

/// Every run of characters `run` accepts, replaced by one `by`.
pub(crate) fn collapse(text: &str, run: impl Fn(char) -> bool, by: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut inside = false;
    for c in text.chars() { if run(c) { if !inside { out += by; } inside = true; } else { out.push(c); inside = false; } }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Every pattern of the import (and of `strptime` for a few formats), checked against the regex crate it replaced.
    const PATTERNS: &[&str] = &[
        r"^(?:total|totaux|sous[- ]?totaux?|sub[- ]?totals?|grand[- ]total|totale?s? général)\b", r"^(?:sources?|notes?|remarques?|n\.\s?b\b|nb\s?:|\*)",
        r"^([0-9]{1,2})[-/. ]([0-9]{4})$|^([0-9]{4})[-/. ]([0-9]{1,2})$|^([0-9]{4})(0[1-9]|1[0-2])$",
        r"^[0-9]{1,4}[-/.][0-9]{1,2}[-/.][0-9]{1,4}([ T][0-9]{1,2}:[0-9]{2}(:[0-9]{2}(\.[0-9]+)?)?(Z|[+-][0-9]{2}:?[0-9]{2})?)?$",
        r"^([0-9]{1,4})[-/.]([0-9]{1,2})[-/.]([0-9]{1,4})", r"^[-+(]?\s*\d[\d\s\u{a0}\u{202f}.,]*\)?\s*[%€$£]?$",
        r"(?i)\b(mois|month|periode|période|period|tperiode)\b", r"(?i)\b(date|jour|day|echeance|échéance)\b", r"(?i)\b(annee|année|year|exercice)\b",
        r"(?i)\b(code|id|ref|réf|reference|référence|numero|numéro|num|no|n°|siret|siren|cp|postal|zip|tel|téléphone|phone|iban|compte|account)s?\b",
        r"(?i)\b(client|customer|fournisseur|supplier|vendor|article|produit|product|item|sku|societe|société|company|entite|entité|entity|site|magasin|store|agence|branch|salarie|salarié|employee|matricule|projet|project|commande|order|facture|invoice)s?\b",
        "^[+-]?[0-9]+(_[0-9]+)+(\\.[0-9]+)?$", r"^[0-9]{1,2}[-/.][0-9]{1,2}[-/.][0-9]{2,4}\b", "^([0-9]{1,2})[-/. ]([0-9]{4})$", "^([0-9]{4})[-/. ]?([0-9]{2})$",
        r"^[T ]?([0-9]{1,2})(?::?([0-9]{2})(?::?([0-9]{2})(?:[.,]([0-9]{1,9}))?)?)?\s*(Z|[+-][0-9]{2}:?[0-9]{2})?$",
        r"^([0-9]{4})(?:-([0-9]{1,2})(?:-([0-9]{1,2}))?|([0-9]{2})([0-9]{2}))?((?:[T ][0-9].*)?)$", r"^([0-9]{1,4})([-/. ])([0-9]{1,2})([-/. ])([0-9]{1,4})((?:[T ][0-9].*)?)$",
        r"^([0-9]{1,2})[-/]([0-9]{4})$", "^[+-]?[0-9]+,[0-9]+$", r"^[+-]?[0-9]{1,3}(?:[.,][0-9]{3})+(?:[.,][0-9]+)?$", r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$",
        "^[0-9]{4}-[0-9]{2}-[0-9]{2}$", r"^([0-9]{4})-([0-9]{2})-([0-9]{2})[T ]([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.([0-9]+))?)?(?:([+-])([0-9]{2}):([0-9]{2}))?$",
        r"(?i)^(?P<d>3[01]|[12][0-9]|0[1-9]|[1-9]| [1-9])/(?P<m>1[0-2]|0[1-9]|[1-9])/(?P<Y>[0-9]{4})$", r"(?i)^(?P<y>[0-9]{2})(?P<m>1[0-2]|0[1-9]|[1-9])(?P<d>3[01]|[12][0-9]|0[1-9]|[1-9]| [1-9])$",
        r"(?i)^(?P<d>3[01]|[12][0-9]|0[1-9]|[1-9]| [1-9])\s+(?P<b>[a-z]+)\s+(?P<Y>[0-9]{4})$", r"(?i)^(?P<Y>[0-9]{4})\-(?P<m>1[0-2]|0[1-9]|[1-9])\-(?P<d>3[01]|[12][0-9]|0[1-9]|[1-9]| [1-9])\s+(?P<H>2[0-3]|[01][0-9]|[0-9])\:(?P<M>[0-5][0-9]|[0-9])\:(?P<S>6[01]|[0-5][0-9]|[0-9])\.(?P<f>[0-9]{1,6})$",
    ];

    const SAMPLES: &[&str] = &[
        "", "0", "12", "2025", "202501", "202513", "01-2025", "1/2025", "2025 01", "2025-1", "2025-01-15", "2025-1-5", "15/01/2025", "15.01.25", "1/2/3", "15/01/2025 10:30",
        "2025-01-15T10:30:00", "2025-01-15T10:30:00.5Z", "2025-01-15 10:30:00.123456+02:00", "2025-01-15T10:30:00+0200", "10:30", "T10", " 7:05:09,25 Z", "1030", "103000",
        "20250203", "700203", "3/2/2025", "31/12/1999", " 3/2/2025", "3 February 2025", "03 feb 2025", "2025-02-03 07:45:00.25", "1 234,56", "1.234,56", "1,234.56", "(120)",
        "16,2%", "1234.56 €", "-5", "+5", ".5", "5.", "1e3", "1_000", "1_000_000.5", "--5", "1.2.3", "12 345", "1\u{a0}000,5", "1\u{202f}000", "(1 234)", "١٢٣", "１２３",
        "Total", "TOTAL général", "Sous-total", "sous totaux", "Subtotal", "sub-totals", "Grand total", "Totales général", "totalité", "Source : Insee", "Notes", "N.B. voir",
        "nb: rien", "* hors taxe", "Remarques", "NB", "n.b", "Code client", "ID", "réf", "N° facture", "no", "Numéros", "CP", "Clients", "Période", "TPERIODE", "Mois_comptable",
        "date échéance", "Échéance", "Année", "exercices", "Société", "entités", "codes", "client_id", "idée", "Kode", "ſite", "Kelvin K", "a\tb  c", "x y\n", "2025-01-05Z",
    ];

    fn same(ours: &Pattern, oracle: &regex::Regex, text: &str) {
        let expected = oracle.captures(text).map(|found| found.iter().map(|group| group.map(|group| group.as_str().to_owned())).collect::<Vec<_>>());
        let actual = ours.captures(text).map(|found| (0..ours.slots / 2).map(|index| found.get(index).map(|group| group.as_str().to_owned())).collect::<Vec<_>>());
        assert_eq!(actual, expected, "{oracle:?} on {text:?}");
        for name in oracle.capture_names().flatten() { assert_eq!(ours.captures(text).and_then(|found| found.name(name).map(|m| m.as_str())), oracle.captures(text).and_then(|found| found.name(name).map(|m| m.as_str())), "{oracle:?} {name} on {text:?}"); }
    }

    #[test] fn samples_match_as_the_regex_crate() { for pattern in PATTERNS { let (ours, oracle) = (Pattern::new(pattern).unwrap(), regex::Regex::new(pattern).unwrap()); for text in SAMPLES { same(&ours, &oracle, text); } } }
    #[test] fn long_inputs_stay_linear() { let text = format!("1{}x", " 1".repeat(50_000)); assert!(!pattern!(r"^[-+(]?\s*\d[\d\s\u{a0}\u{202f}.,]*\)?\s*[%€$£]?$").is_match(&text)); assert!(!Pattern::new("(a*)*b").unwrap().is_match(&"a".repeat(10_000))); assert!(pattern!(r"(?i)\b(code)\b").is_match(&format!("{} code", "é".repeat(50_000)))); }
    #[test] fn collapse_replaces_runs() { assert_eq!(collapse("a \t b\t\tc ", |c| c == ' ' || c == '\t', " "), "a b c "); assert_eq!(collapse("code_client--x.y", |c| matches!(c, '_' | '-' | '.'), " "), "code client x y"); }

    #[test] #[ignore] fn bench_patterns() { let values = ["1700000123", "11/03/2025", "03-2025", "4 140,02", "42", "Article 12 lot 4567", "Nord", "oui", "Code client"]; for pattern in PATTERNS { let (ours, oracle) = (Pattern::new(pattern).unwrap(), regex::Regex::new(pattern).unwrap()); let t = std::time::Instant::now(); for _ in 0..20_000 { for v in values { std::hint::black_box(ours.captures(v).is_some()); } } let a = t.elapsed(); let t = std::time::Instant::now(); for _ in 0..20_000 { for v in values { std::hint::black_box(oracle.captures(v).is_some()); } } let b = t.elapsed(); let t = std::time::Instant::now(); for _ in 0..20_000 { for v in values { std::hint::black_box(ours.is_match(v)); } } let c = t.elapsed(); let t = std::time::Instant::now(); for _ in 0..20_000 { for v in values { std::hint::black_box(oracle.is_match(v)); } } println!("{:>6.1} {:>6.1} | {:>6.1} {:>6.1} ms  {}", a.as_secs_f64() * 1e3, b.as_secs_f64() * 1e3, c.as_secs_f64() * 1e3, t.elapsed().as_secs_f64() * 1e3, &pattern[..pattern.len().min(60)]); } }

    proptest! {
        #[test] fn random_texts_match_as_the_regex_crate(text in "[0-9 .,/:_+\\-eETZ%€()a-zé°\u{a0}*]{0,24}") { for pattern in PATTERNS { same(&Pattern::new(pattern).unwrap(), &regex::Regex::new(pattern).unwrap(), &text); } }
        #[test] fn token_texts_match_as_the_regex_crate(text in "([0-9]{1,4}|[-/. T:+Z,%()€_]|\\.[0-9]{1,7}|e|Sous-|total|N°|code|s|feb| [1-9]){0,9}") { for pattern in PATTERNS { same(&Pattern::new(pattern).unwrap(), &regex::Regex::new(pattern).unwrap(), &text); } }
    }
}
