#![cfg_attr(rustfmt, rustfmt_skip)]
//! Search through everything a Place holds (RECHERCHE_PLACE in wip/README.md): its data, its files and folders, its Apps, and any
//! content to come. A source turns one kind of content into entries (a text, what it refers to, the scope keys it falls
//! under); the index finds them by their words, globally or within a scope, whatever their source.
//!
//! The text functions are always compiled (the values of `service/values.rs` match with them); the index and its
//! sources come with the `search` feature, the readers of PDF and office documents with `search-documents`.

#[cfg(feature = "search")]
pub mod extract;
#[cfg(feature = "search")]
mod index;
#[cfg(feature = "search")]
pub mod sources;
#[cfg(feature = "search")]
pub use index::{search, Entry, Hit, Origin, Part};

/// Lower case, accents folded, every other character a space, single spaces.
pub fn normalize(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars().flat_map(char::to_lowercase) {
        let folded = match character {
            'à' | 'á' | 'â' | 'ä' | 'ã' | 'å' => "a", 'ç' => "c", 'è' | 'é' | 'ê' | 'ë' => "e", 'ì' | 'í' | 'î' | 'ï' => "i",
            'ñ' => "n", 'ò' | 'ó' | 'ô' | 'ö' | 'õ' => "o", 'ù' | 'ú' | 'û' | 'ü' => "u", 'ý' | 'ÿ' => "y", 'œ' => "oe", 'æ' => "ae", 'ß' => "ss",
            other if other.is_alphanumeric() => { out.push(other); continue; }
            _ => " ",
        };
        out.push_str(folded);
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// At most one edit (two from eight letters) between two words of five letters or more.
pub fn near(a: &str, b: &str) -> bool {
    let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    if a.len() < 5 || b.len() < 5 { return false; }
    let limit = if a.len().min(b.len()) >= 8 { 2 } else { 1 };
    if a.len().abs_diff(b.len()) > limit { return false; }
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.iter().enumerate() {
        let mut current = vec![i + 1; b.len() + 1];
        for (j, cb) in b.iter().enumerate() { current[j + 1] = (previous[j] + usize::from(ca != cb)).min(previous[j + 1] + 1).min(current[j] + 1); }
        previous = current;
    }
    previous[b.len()] <= limit
}

/// Words that never find anything on their own: linking and question words, French and English.
const STOP: &[&str] = &["le", "la", "les", "l", "un", "une", "des", "du", "de", "d", "a", "au", "aux", "en", "et", "ou", "par", "pour", "sur", "dans", "avec", "sans", "qui", "que", "quoi", "quel", "quelle", "quels", "quelles",
    "est", "c", "ce", "cet", "cette", "ces", "mon", "ma", "mes", "notre", "nos", "je", "j", "tu", "il", "y", "moi", "me", "se", "s", "qu", "n", "ne", "pas", "trouve", "trouver", "cherche", "chercher", "retrouve",
    "retrouver", "montre", "affiche", "donne", "combien", "comment", "oui", "merci", "svp", "stp", "bonjour", "yes", "please", "thanks", "the", "an", "of", "and", "or", "for", "in", "on", "to", "with", "is", "are", "what", "which", "where", "who", "show", "find", "search", "my", "our"];

/// The words of a text as the index keeps them: normalised, a run of single letters joined (« T.V.A. » → `tva`), a final
/// plural `s` or `x` dropped, linking words left out, each once.
pub fn words(text: &str) -> Vec<String> { counted(text).into_iter().map(|(word, _)| word).collect() }

/// [`words`] with the times each occurs, in the order they first do.
pub fn counted(text: &str) -> Vec<(String, u32)> {
    let normalized = normalize(text);
    let (mut out, mut letters) = (Vec::<(String, u32)>::new(), String::new());
    let keep = |word: &str, out: &mut Vec<(String, u32)>| {
        let word = if word.len() > 4 && (word.ends_with('x') || (word.ends_with('s') && !word.ends_with("ss"))) { &word[..word.len() - 1] } else { word };
        if STOP.contains(&word) { return; }
        match out.iter_mut().find(|(known, _)| known == word) { Some((_, times)) => *times += 1, None => out.push((word.to_owned(), 1)) }
    };
    for word in normalized.split(' ').filter(|word| !word.is_empty()) {
        if word.len() == 1 && word.as_bytes()[0].is_ascii_alphabetic() { letters.push_str(word); continue; }
        if !letters.is_empty() { keep(&std::mem::take(&mut letters), &mut out); }
        keep(word, &mut out);
    }
    if !letters.is_empty() { keep(&letters, &mut out); }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn words_fold_acronyms_plurals_and_linking_words() { assert_eq!(words("C'est quoi la T.V.A. collectée des comptes ?"), ["tva", "collectee", "compte"]); assert_eq!(words("Taux de remise, frais"), ["taux", "remise", "frai"]); }
}
