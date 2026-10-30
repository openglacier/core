//! Query execution and event subscription requests.
#![cfg_attr(rustfmt, rustfmt_skip)]

use super::*;
use super::serve::{place_role_denied, authorization_denied, diverged, reply, Connection, Handled};

#[cfg(feature = "database")]
pub(super) fn handle_query_operation(connection: &mut Connection, settings: &ConnectionSettings, engine: &Engine, event_engine: &EventEngine, delegation: Option<&GatewayDelegation>, operation: RoutedOperation, compact: &mut bool) -> Handled {
    let RoutedOperation::QueryExecute(Routed { id, input: QueryExecuteInput { query, context, read_only, params, .. } }) = operation else {
        return diverged(operation.id());
    };
    let writer = &mut connection.writer;
    let principal = connection.authentication.principal();
    debug::log( DebugTopic::Query, Some(connection.id), format!("id={id} {}", query), );
    let started = (Instant::now(), crate::helpers::now_millis());
    let traced = settings.query_trace.is_some().then(|| (query.clone(), params.clone()));

    // `explain <query>` goes through the same checks as `<query>`
    // but returns its plan instead of running it.
    let (explain, query) = match crate::query::split_explain(&query) {
        Some(inner) => (true, inner.to_owned()),
        None => (false, query),
    };

    // Place context is optional for backward compatibility; AppInstance is an optional sub-scope.
    // When supplied, it is untrusted wire data until og-core resolves it
    // against the authenticated principal and persisted system records.
    let execution_context = match context {
        Some(requested) => {
            let delegated_context = delegation.and_then(|value| {
                if value.capability == "database"
                    && value.place_id.as_deref() == Some(requested.place_id.as_str())
                    && value.app_instance_id.as_deref() == requested.app_instance_id.as_deref()
                {
                    value.access().map(|access| ExecutionContext {
                        principal: value.principal.clone(),
                        place_id: requested.place_id.clone(),
                        app_instance_id: requested.app_instance_id.clone(),
                        access,
                        filters: value.filters.clone(),
                    })
                } else { None }
            });
            match delegated_context {
                Some(context) => Some(context),
                None => Some(resolve_query_execution_context(engine, id, principal, requested)?),
            }
        },
        None => None,
    };

    // A query reading its Place (views, `let` queries) is prepared once the
    // Place is known.
    let query = match params {
        Some(parameters) => match crate::view::prepare(engine, execution_context.as_ref().map(ExecutionContext::scope).as_ref(), &query, &parameters) {
            Ok(prepared) => prepared,
            Err(message) => reject!(id, "query.invalid", message),
        },
        None => query,
    };
    let query = match crate::view::literals(engine, &query) {
        Ok(query) => query,
        Err(message) => reject!(id, "query.invalid", message),
    };
    let prepare_us = elapsed_micros(started.0);

    // `define view` / `undefine view` write `_views` for the Place Owner, or a
    // standard view for a principal allowed to write `_views`.
    if let Some(statement) = crate::view::Statement::parse(&query) {
        let allowed = match execution_context.as_ref() {
            Some(context) => context.access.role() == Some(PlaceRole::Owner),
            None => !settings.authorization_mode.is_enforced() || authorize_connection(engine, principal, AuthorizationAction::QueryWrite, crate::view::VIEWS_COLLECTION),
        };
        if read_only && !explain {
            reject!(id, "query.read_only_violation", "readOnly query attempted to define a view");
        }
        if !allowed {
            reject!(id, "view.forbidden", "views are defined by the Owner of the Place");
        }
        let author = principal_actor(execution_context.as_ref().map_or(principal, |context| &context.principal));
        return match statement.and_then(|statement| statement.run(engine, execution_context.as_ref().map(|context| context.place_id.as_str()), &author, explain)) {
            Ok(view) => Err(QueryResponse::success(id, vec![view], None).into()),
            Err(message) => reject!(id, "view.invalid", message),
        };
    }

    if read_only {
        let readonly_plan = parse_pipeline(&query)
            .and_then(|pipeline| {
                crate::query::Planner::new()
                    .plan(&pipeline)
                    .map_err(|error| QueryTextError::Planning(error.to_string()))
            });
        match readonly_plan {
            Ok(plan) if !plan.is_write() => {}
            Ok(_) => reject!(id, "query.read_only_violation", "readOnly query attempted to mutate storage"),
            Err(error) => reject!(id, "query.invalid", error.to_string()),
        }
    }

    let analyzed_access = QueryAccess::analyze(&query).ok();
    // Only og-core writes the cache of materialized views.
    if analyzed_access.as_ref().is_some_and(|access| access.action == AuthorizationAction::QueryWrite && access.collection.starts_with(crate::view::CACHE_COLLECTION)) {
        reject!(id, "query.invalid", "the cache of materialized views is written by og-core only");
    }
    let grows = analyzed_access.as_ref().is_some_and(|access| access.grows);
    let scoped_write_collection = analyzed_access.as_ref()
        .filter(|access| access.action == AuthorizationAction::QueryWrite)
        .map(|access| access.collection.clone());

    // The resolved Place grant is the capability ceiling for a scoped Place
    // query: every grant reads; writes need a writing role (Resident, Owner)
    // or a read/write public policy, before the scoped grant is considered.
    if let (Some(context), Some(access)) =
        (execution_context.as_ref(), analyzed_access.as_ref())
    {
        if access.action == AuthorizationAction::QueryWrite
            && !context.access.can_write()
        {
            write_response(writer, &place_role_denied(id, context, access.action, &access.collection))?;
            event_engine.publish_global(
                "authorization.denied",
                serde_json::json!({
                    "requestId": id,
                    "action": access.action.as_str(),
                    "resource": access.collection,
                    "principal": principal,
                    "placeId": context.place_id,
                    "appInstanceId": context.app_instance_id,
                    "placeRole": context.access.role().map(PlaceRole::as_str),
                    "publicAccess": context.access.public_access().map(PublicAccess::as_str),
                }),
            );
            return Ok(());
        }
    }

    if settings.authorization_mode.is_enforced() {
        let Some(access) = analyzed_access else {
            return Err(execute_request(engine, QueryRequest::new(id, query)).into());
        };

        // A validated Place context is itself the grant for ordinary App
        // collections. When an AppInstance is supplied it narrows the scope.
        // The physical executor applies the trusted DocumentScope below the
        // query language, so this grant can never escape the requested Place.
        // Place roles remain
        // the capability ceiling: Member=read, Resident/Owner=read+write.
        // System collections keep the explicit global permission model.
        // A query ending in `into` also reads its source collection.
        let checks = std::iter::once((access.action, access.collection.as_str()))
            .chain(access.source_read.as_deref().map(|source| (AuthorizationAction::QueryRead, source)));
        for (action, resource) in checks {
            // A Place reads its own views (`_views`, restricted below to its
            // rows) and its cached rows of materialized views; anyone reads
            // the constant rows of `values […]`.
            let reading = action == AuthorizationAction::QueryRead;
            let scoped_app_collection = execution_context.is_some()
                && (!resource.starts_with('_') || (reading && (resource == crate::view::VIEWS_COLLECTION || resource.starts_with(crate::view::CACHE_PREFIX))));
            if !scoped_app_collection && !(reading && resource.starts_with(crate::view::VALUES_PREFIX))
                && !authorize_connection(engine, principal, action, resource)
            {
                write_response(writer, &authorization_denied(id, principal, action, resource))?;
                event_engine.publish_global(
                    "authorization.denied",
                    serde_json::json!({
                        "requestId": id,
                        "action": action.as_str(),
                        "resource": resource,
                        "principal": principal,
                        "placeId": execution_context.as_ref().map(|context| &context.place_id),
                        "appInstanceId": execution_context.as_ref().and_then(|context| context.app_instance_id.as_deref()),
                    }),
                );
                return Ok(());
            }
        }
    }
    if explain {
        return Err(explain_request(engine, id, &query).into());
    }
    // Past a storage limit, a Place takes no new documents (step 3 of POLICIES in wip/README.md).
    #[cfg(feature = "auth")]
    if let Some(context) = execution_context.as_ref().filter(|_| grows) {
        super::usage_ops::check_storage(engine, id, &context.place_id, 0)?;
    }
    // An instance of an App its Place Owner's policy no longer includes is read-only (step 4).
    #[cfg(feature = "auth")]
    if settings.enrollment_mode.is_policy() && scoped_write_collection.is_some() {
        if let Some(context) = execution_context.as_ref() {
            match (context.app_instance_id.as_deref(), scoped_write_collection.as_deref()) {
                (Some(instance_id), _) => super::policy_ops::check_instance_write(engine, id, &context.place_id, instance_id)?,
                // Outside any instance, the collection written tells which App it belongs to.
                (None, Some(collection)) => super::policy_ops::check_collection_write(engine, id, &context.place_id, collection)?,
                (None, None) => {}
            }
        }
    }
    let request = QueryRequest::new(id, query);
    *compact = request.query.contains("LOAD") || request.query.contains("load");
    event_engine.publish_global(
        "query.started",
        serde_json::json!({
            "requestId": id,
            "principal": principal,
            "context": execution_context.as_ref().map(|context| serde_json::json!({
                "placeId": context.place_id,
                "appInstanceId": context.app_instance_id,
                "placeRole": context.access.role().map(PlaceRole::as_str),
                "publicAccess": context.access.public_access().map(PublicAccess::as_str),
            })),
        }),
    );
    // A scoped read streams like any read: the trusted document scope is a
    // filter written into its plan, in front of every source it reads,
    // below query syntax. Scoped writes return a whole response.
    let scope = execution_context.as_ref().map(ExecutionContext::scope);
    // A Place read of rows kept or being read for another reader is not run again.
    let key = execution_context.as_ref().filter(|_| scoped_write_collection.is_none()).and_then(|context| result_key(engine, &request.query, context));
    let prepared = traced.as_ref().map_or(0, |_| crate::helpers::fnv1a64(request.query.as_bytes()));
    let connection_id = connection.id;
    let trace = |cache: &str, ok: bool| trace(settings, &started, prepare_us, connection_id, principal, execution_context.as_ref(), traced, prepared, cache, ok);
    let (lead, cache) = match key.map(|key| engine.results().begin(key)) {
        Some(Lookup::Hit(rows)) => {
            write_kept_rows(writer, id, &rows)?;
            trace("hit", true);
            event_engine.publish_global("query.finished", serde_json::json!({ "requestId": id }));
            return Ok(());
        }
        Some(Lookup::Lead(lead)) => (Some(lead), "lead"),
        Some(Lookup::Run) => (None, "run"),
        None => (None, "none"),
    };
    let mut capture = lead.as_ref().map(|_| Rows::new(true));
    let streaming = try_write_streaming_request(engine, writer, &request, scope.as_ref(), &mut capture);
    match streaming {
        Ok(true) => {
            if let Some(lead) = lead {
                lead.finish(capture);
            }
            trace(cache, true);
            event_engine.publish_global(
                "query.finished",
                serde_json::json!({ "requestId": id }),
            );
        }
        Ok(false) => {
            // Commits made by this query are recorded with its author
            // (`_by` / `_by_device` in `history`), when history is kept.
            let author = commit_author(execution_context.as_ref().map_or_else(|| delegation.map_or(principal, |value| &value.principal), |context| &context.principal));
            let response = crate::storage::history::with_commit_author(author, || execute_request_scoped(
                engine,
                request,
                execution_context.as_ref(),
            ));
            let ok = response.is_ok();
            if let Some(lead) = lead {
                lead.finish(match &response {
                    QueryResponse::Ok { documents, statistics, .. } => {
                        let (mut rows, limit) = (Rows::new(false), engine.results().entry_limit());
                        documents.iter().all(|document| rows.push_value(document.clone(), limit)).then(|| rows.with_statistics(statistics.clone()))
                    }
                    QueryResponse::Error { .. } => None,
                });
            }
            write_response(writer, &response)?;
            trace(cache, ok);
            let event_type = if ok { "query.finished" } else { "query.failed" };
            event_engine.publish_global(
                event_type,
                serde_json::json!({ "requestId": id }),
            );
            if ok {
                if let (Some(context), Some(collection)) =
                    (execution_context.as_ref(), scoped_write_collection.as_ref())
                {
                    #[cfg(feature = "auth")]
                    if !grows { super::usage_ops::note_shrink(&context.place_id); }
                    let audience = place_audience(engine, id, &context.place_id)
                        .unwrap_or(Audience::Global);
                    publish_durable_event(
                        engine,
                        audience,
                        "app.data.changed",
                        serde_json::json!({
                            "placeId": context.place_id,
                            "appInstanceId": context.app_instance_id,
                            "collection": collection,
                        }),
                    );
                }
            }
        }
        Err(error) => {
            event_engine.publish_global("query.failed", serde_json::json!({ "requestId": id, "message": error.to_string() }));
            return Err(error.into());
        }
    }
    Ok(())
}

#[cfg(feature = "events")]
pub(super) fn handle_subscription_operation(connection: &mut Connection, settings: &ConnectionSettings, event_engine: &EventEngine, operation: RoutedOperation) -> Handled {
    let RoutedOperation::EventsSubscribe(Routed { id, input: EventsSubscribeInput { mut types } }) = operation else {
        return diverged(operation.id());
    };
    if settings.authenticated_keepalive
        && !matches!(connection.authentication.principal(), Principal::Anonymous)
    {
        ensure_authenticated_keepalive_type(&mut types);
    }
    debug::log( DebugTopic::Events, Some(connection.id), format!("subscribe types={types:?}"), );
    let subscription_id = connection.subscribe(event_engine.subscribe(types))?;
    reply(&mut connection.writer, id, serde_json::json!({ "subscribed": true, "subscriptionId": subscription_id }))?;
    event_engine.publish_global(
        "core.started",
        serde_json::json!({ "subscriptionId": subscription_id }),
    );
    Ok(())
}

/// The key of a Place read's rows in the result cache (`engine/results.rs`):
/// its prepared text, its scope and access filters, and the change tokens of
/// the collections it reads. `None` when its rows cannot be kept: it writes,
/// calls `now()`, or the storage cannot tell what changed.
pub(super) fn result_key(engine: &Engine, query: &str, context: &ExecutionContext) -> Option<String> {
    if query.split("now").skip(1).any(|rest| rest.trim_start().starts_with('(')) {
        return None;
    }
    let planned = engine.plan(&parse_pipeline(query).ok()?).ok()?;
    if planned.physical().is_write() {
        return None;
    }
    let mut reads = vec![planned.physical().source().collection().to_string()];
    crate::view::collect_reads(planned.physical().operators(), &mut reads);
    let tokens = reads.iter().map(|read| engine.change_token(read, Some(&context.place_id))).collect::<Option<Vec<_>>>()?;
    Some(format!("{}\n{}\n{:?}\n{}\n{query}", context.place_id, context.app_instance_id.as_deref().unwrap_or_default(), context.filters, serde_json::Value::from(tokens)))
}

/// One line of `OGD_QUERY_TRACE` for a query sent back: what was asked, in
/// which scope, what ran (`prepared`: a hash of the prepared text) and
/// what the result cache did (`hit`, `lead`, `run`, `none`).
#[allow(clippy::too_many_arguments)]
fn trace(settings: &ConnectionSettings, started: &(Instant, i64), prepare_us: u64, connection: u64, principal: &Principal, context: Option<&ExecutionContext>, asked: Option<(String, Option<serde_json::Map<String, serde_json::Value>>)>, prepared: u64, cache: &str, ok: bool) {
    let (Some(file), Some((query, params))) = (settings.query_trace.as_ref(), asked) else { return };
    let line = serde_json::json!({
        "t": started.1, "us": elapsed_micros(started.0), "prepare_us": prepare_us, "connection": connection,
        "who": principal_actor(context.map_or(principal, |context| &context.principal)),
        "place": context.map(|context| &context.place_id), "app": context.and_then(|context| context.app_instance_id.as_deref()),
        "filters": context.map(|context| format!("{:?}", context.filters)), "query": query, "params": params,
        "prepared": format!("{prepared:016x}"), "cache": cache, "ok": ok,
    });
    let _ = writeln!(crate::helpers::lock_unpoisoned(file), "{line}");
}

/// The commit author of a principal (`None` when anonymous).
fn commit_author(principal: &Principal) -> Option<crate::storage::history::CommitAuthor> {
    match principal {
        Principal::Anonymous => None,
        Principal::Identity { identity_id, device_id } => Some(crate::storage::history::CommitAuthor {
            identity: std::sync::Arc::from(identity_id.as_str()),
            device: (!device_id.is_empty()).then(|| std::sync::Arc::from(device_id.as_str())),
        }),
    }
}
