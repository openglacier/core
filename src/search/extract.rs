#![cfg_attr(rustfmt, rustfmt_skip)]
//! The text of a file, by its extension: plain text formats always; PDF, spreadsheets (xlsx, xls, ods) and office
//! documents (docx, pptx, odt, odp) with the `search-documents` feature. A document a reader fails on (or panics on, a
//! malformed PDF) gives no text, never an error: its name is still found.

/// Files larger than this are not read.
pub const MAX_BYTES: u64 = 32 << 20;
/// The text kept of one file.
const MAX_CHARS: usize = 2 << 20;
/// The length a passage aims at, cut at a space.
pub const PASSAGE: usize = 1_000;

const PLAIN: &[&str] = &["txt", "md", "markdown", "csv", "tsv", "json", "jsonl", "log", "ini", "toml", "yaml", "yml", "sql", "rst", "tex"];
const MARKUP: &[&str] = &["html", "htm", "xml", "svg"];
#[cfg(feature = "search-documents")]
const DOCUMENTS: &[&str] = &["pdf", "xlsx", "xlsm", "xls", "ods", "docx", "pptx", "odt", "odp"];
#[cfg(not(feature = "search-documents"))]
const DOCUMENTS: &[&str] = &[];

fn extension(name: &str) -> String { name.rsplit_once('.').map(|(_, extension)| extension.to_ascii_lowercase()).unwrap_or_default() }

/// Whether the text of a file of this name can be read.
#[must_use]
pub fn readable(name: &str) -> bool { let extension = extension(name); [PLAIN, MARKUP, DOCUMENTS].iter().any(|kinds| kinds.contains(&extension.as_str())) }

/// The text of the file `name` holding `bytes` (`None`: unreadable).
#[must_use]
pub fn text(name: &str, bytes: &[u8]) -> Option<String> {
    let extension = extension(name);
    let mut text = match extension.as_str() {
        kind if PLAIN.contains(&kind) => decode(bytes),
        kind if MARKUP.contains(&kind) => markup(&decode(bytes)),
        #[cfg(feature = "search-documents")]
        kind if DOCUMENTS.contains(&kind) => std::panic::catch_unwind(|| documents::text(kind, bytes)).ok().flatten()?,
        _ => return None,
    };
    if text.len() > MAX_CHARS { let mut end = MAX_CHARS; while !text.is_char_boundary(end) { end -= 1; } text.truncate(end); }
    Some(text)
}

/// UTF-8, else Windows-1252 read as Latin-1 (what a French CSV export is).
fn decode(bytes: &[u8]) -> String {
    let bytes = bytes.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(bytes);
    match std::str::from_utf8(bytes) { Ok(text) => text.to_owned(), Err(_) => bytes.iter().map(|&byte| char::from(byte)).collect() }
}

/// The text of HTML or XML: tags become spaces, entities are decoded, scripts and styles left out.
#[must_use]
pub fn markup(source: &str) -> String {
    let (mut out, mut rest) = (String::with_capacity(source.len()), source);
    while let Some(at) = rest.find('<') {
        out.push_str(&entities(&rest[..at]));
        let tag = &rest[at..];
        let lower = tag.get(..8).unwrap_or(tag).to_ascii_lowercase();
        let skip = ["<script", "<style"].iter().find(|open| lower.starts_with(**open)).and_then(|open| tag.to_ascii_lowercase().find(&format!("</{}", &open[1..])));
        let end = match skip { Some(close) => tag[close..].find('>').map(|end| close + end), None => tag.find('>') };
        let Some(end) = end else { rest = ""; break };
        out.push(' ');
        rest = &tag[end + 1..];
    }
    out.push_str(&entities(rest));
    out
}

fn entities(text: &str) -> String {
    if !text.contains('&') { return text.to_owned(); }
    let (mut out, mut rest) = (String::with_capacity(text.len()), text);
    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        let tail = &rest[at..];
        let decoded = tail.find(';').filter(|end| *end <= 10).and_then(|end| {
            let name = &tail[1..end];
            let character = match name { "amp" => Some('&'), "lt" => Some('<'), "gt" => Some('>'), "quot" => Some('"'), "apos" => Some('\''), "nbsp" => Some(' '),
                _ => name.strip_prefix("#x").or_else(|| name.strip_prefix("#X")).map_or_else(|| name.strip_prefix('#').and_then(|code| code.parse().ok()), |code| u32::from_str_radix(code, 16).ok()).and_then(char::from_u32) };
            Some((character?, end))
        });
        match decoded { Some((character, end)) => { out.push(character); rest = &tail[end + 1..]; } None => { out.push('&'); rest = &tail[1..]; } }
    }
    out.push_str(rest);
    out
}

/// The text cut in passages of about [`PASSAGE`] characters, at spaces, blank runs folded.
#[must_use]
pub fn passages(text: &str) -> Vec<String> {
    let (mut out, mut current) = (Vec::new(), String::new());
    for word in text.split_whitespace() {
        if !current.is_empty() && current.len() + word.len() >= PASSAGE { out.push(std::mem::take(&mut current)); }
        if !current.is_empty() { current.push(' '); }
        current.push_str(word);
    }
    if !current.is_empty() { out.push(current); }
    out
}

#[cfg(feature = "search-documents")]
mod documents {
    use std::io::{Cursor, Read};

    pub(super) fn text(kind: &str, bytes: &[u8]) -> Option<String> {
        match kind {
            "pdf" => pdf_extract::extract_text_from_mem(bytes).ok(),
            "xlsx" | "xlsm" | "xls" | "ods" => sheets(bytes),
            "docx" => parts(bytes, |name| name == "word/document.xml" || name.starts_with("word/header") || name.starts_with("word/footer")),
            "pptx" => parts(bytes, |name| name.starts_with("ppt/slides/slide") && name.ends_with(".xml")),
            "odt" | "odp" => parts(bytes, |name| name == "content.xml"),
            _ => None,
        }
    }

    /// Each sheet: its name, then its rows, cells apart by tabs.
    fn sheets(bytes: &[u8]) -> Option<String> {
        use calamine::Reader;
        let mut book = calamine::open_workbook_auto_from_rs(Cursor::new(bytes)).ok()?;
        let mut out = String::new();
        for name in book.sheet_names().clone() {
            let Ok(range) = book.worksheet_range(&name) else { continue };
            out.push_str(&name); out.push('\n');
            for row in range.rows() {
                for cell in row { out.push_str(&cell.to_string()); out.push('\t'); }
                out.push('\n');
            }
        }
        Some(out)
    }

    /// The text of the XML parts of an office archive whose name `wanted` accepts, in their order in the archive.
    fn parts(bytes: &[u8], wanted: impl Fn(&str) -> bool) -> Option<String> {
        let mut archive = zip::ZipArchive::new(Cursor::new(bytes)).ok()?;
        let mut out = String::new();
        for at in 0..archive.len() {
            let mut part = archive.by_index(at).ok()?;
            if !wanted(part.name()) || part.size() > super::MAX_BYTES { continue; }
            let mut xml = String::new();
            if part.read_to_string(&mut xml).is_ok() { out.push_str(&super::markup(&xml)); out.push('\n'); }
        }
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn plain_and_markup_texts_are_read_and_cut_in_passages() { assert_eq!(text("a.CSV", b"\xEF\xBB\xBFcompte;libell\xC3\xA9").as_deref(), Some("compte;libellé")); assert_eq!(text("a.txt", b"caf\xE9").as_deref(), Some("café")); assert_eq!(text("a.html", b"<p>T.V.A.&nbsp;collect&#233;e</p><script>var x = '<b>';</script>&amp; plus").map(|text| text.split_whitespace().collect::<Vec<_>>().join(" ")).as_deref(), Some("T.V.A. collectée & plus")); assert!(text("a.exe", b"x").is_none() && !readable("a.bin") && readable("Notes.MD")); let long = "mot ".repeat(600); let cut = passages(&long); assert_eq!((cut.len(), cut.iter().all(|passage| passage.len() < PASSAGE)), (3, true)); }
    #[cfg(feature = "search-documents")]
    #[test] fn office_documents_and_bad_pdfs_are_read_or_give_nothing() { use std::io::Write; let mut archive = zip::ZipWriter::new(std::io::Cursor::new(Vec::new())); archive.start_file("word/document.xml", zip::write::SimpleFileOptions::default()).unwrap(); archive.write_all(b"<w:document><w:body><w:p><w:t>D\xC3\xA9claration de TVA</w:t></w:p></w:body></w:document>").unwrap(); let bytes = archive.finish().unwrap().into_inner(); assert_eq!(text("d.docx", &bytes).unwrap().split_whitespace().collect::<Vec<_>>(), ["Déclaration", "de", "TVA"]); assert!(text("broken.pdf", b"%PDF-1.4 not really").is_none()); assert!(text("broken.xlsx", b"nope").is_none()); }
}
