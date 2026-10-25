#![cfg_attr(rustfmt, rustfmt_skip)]
//! The decisions of the routing that are not the language model's (wip/APP_MODELE_SEMANTIQUE.md, step 7): the model reads
//! the request and proposes an action (`llm-bench/router.py`); two rules, measured on the real requests, override it.
//!
//! - **Absent data**: a request naming a business concept the Place does not hold (« mes commerciaux », « les commandes »)
//!   is answered « absent », with what is missing; no model recognised one (wave 1, eight models). The concepts and their
//!   words live in `concepts.json`; the Place holds a concept when one of its catalog phrases is among the labels and the
//!   vocabulary of its catalog.
//! - **Follow-up**: after an App, a request that follows it changes that App, unless it asks for a file. The models read
//!   « avec des suggestions », « ajoute un top » as a copy or a new App.

use serde_json::Value as JsonValue;

use crate::helpers::JsonFields;
use crate::service::values::normalize;

#[derive(Debug, Clone)]
pub struct Concept { pub id: String, pub label_fr: String, pub label_en: String, phrases: Vec<Vec<String>>, catalog: Vec<Vec<String>> }

/// Normalised words, a trailing s dropped: « Commerciaux » → `commerciaux`, « devis » → `devi`.
fn words(text: &str) -> Vec<String> { normalize(text).split_whitespace().map(|word| word.trim_end_matches('s').to_owned()).filter(|word| !word.is_empty()).collect() }

pub fn concepts() -> Vec<Concept> {
    let file: JsonValue = serde_json::from_str(include_str!("concepts.json")).expect("concepts.json");
    file.array_field("concepts").map_or(&[][..], Vec::as_slice).iter().map(|concept| {
        let list = |key: &str| concept.array_field(key).map_or(&[][..], Vec::as_slice).iter().filter_map(JsonValue::as_str).map(str::to_owned).collect::<Vec<_>>();
        Concept { id: concept.string_field("id"), label_fr: concept["label"].string_field("fr"), label_en: concept["label"].string_field("en"),
            phrases: list("phrases").iter().map(|phrase| words(phrase)).collect(), catalog: list("catalog").iter().map(|phrase| words(phrase)).collect() }
    }).collect()
}

/// The concepts the request names that the catalog (its text: labels and vocabulary) does not hold.
pub fn absent_concepts<'a>(request: &str, catalog: &str, concepts: &'a [Concept]) -> Vec<&'a Concept> {
    let (request, catalog) = (words(request), words(catalog));
    let has = |text: &[String], phrases: &[Vec<String>]| phrases.iter().any(|phrase| !phrase.is_empty() && text.windows(phrase.len()).any(|window| window == phrase.as_slice()));
    concepts.iter().filter(|concept| has(&request, &concept.phrases) && !has(&catalog, &concept.catalog)).collect()
}

/// The action of a request: absent data first, then a follow-up of an App, else what the model proposed.
pub fn route(request: &str, previous: Option<&str>, proposed: &str, catalog: &str, concepts: &[Concept]) -> String {
    if !absent_concepts(request, catalog, concepts).is_empty() { return "absent".into(); }
    if previous == Some("app") && proposed != "exporter" { return "app".into(); }
    proposed.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    const CATALOG: &str = "ventes.revenue : Chiffre d'affaires (ventes) ; aussi : CA facturé, ventes\nventes.client : Client ; aussi : comptes\nTable finance (indicateurs comptables) ; marge commerciale";
    fn absent(request: &str, catalog: &str) -> Vec<String> { absent_concepts(request, catalog, &concepts()).iter().map(|concept| concept.id.clone()).collect() }

    #[test] fn requests_naming_a_concept_the_place_lacks_are_absent() { assert_eq!(absent("Calculer le niveau de performance de chaque équipe. Et de chaque commercial.", CATALOG), ["commercial", "equipe"]); assert_eq!(absent("Faire une analyse de la performance de mes commerciaux.", CATALOG), ["commercial"]); assert_eq!(absent("peux-tu mesurer les pertes liées à l'annulation des commandes ?", CATALOG), ["commande"]); assert_eq!(absent("Sales by salesperson this year", CATALOG), ["commercial"]); }
    #[test] fn the_adjective_and_a_held_concept_are_not_absent() { assert!(absent("Donne moi une analyse de ma performance commerciale au 31/12/2022", CATALOG).is_empty()); assert!(absent("le résultat commercial du trimestre", CATALOG).is_empty()); assert!(absent("Faire une analyse de la performance de mes commerciaux.", &format!("{CATALOG}\nventes.rep : Commercial ; aussi : vendeur")).is_empty()); assert!(absent("Quels clients paient le plus tard ?", CATALOG).is_empty()); }
    #[test] fn a_follow_up_of_an_app_stays_an_app_unless_it_asks_for_a_file() { let concepts = concepts(); assert_eq!(route("Avec des suggestions d'amélioration", Some("app"), "copier_app", CATALOG, &concepts), "app"); assert_eq!(route("Exporte-le en Excel", Some("app"), "exporter", CATALOG, &concepts), "exporter"); assert_eq!(route("Affiche le CA de cette année", Some("repondre"), "app", CATALOG, &concepts), "app"); assert_eq!(route("et par commercial ?", Some("app"), "app", CATALOG, &concepts), "absent"); }
}
