//! Query execution and event subscription requests.
#![cfg_attr(rustfmt, rustfmt_skip)]

use super::*;
use super::serve::{place_role_denied, authorization_denied, diverged, reply, Connection, Handled};

#[cfg(feature = "database")]
pub(super) fn handle_query_operation(connection: &mut Connection, settings: &ConnectionSettings, engine: &Engine, event_engine: &EventEngine, delegation: Option<&GatewayDelegation>, operation: RoutedOperation, compact: &mut bool) -> Handled {
    let RoutedOperation::QueryExecute(Routed { id, input: QueryExecuteInput { query, context, read_only, .. } }) = operation else {
        return diverged(operation.id());
    };
    let writer = &mut connection.writer;
    let principal = connection.authentication.principal();
    debug::log( DebugTopic::Query, Some(connection.id), format!("id={id} {}", query), );

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

    if read_only {
        let readonly_plan = parse_pipeline(&query)
            .and_then(|pipeline| {
                crate::query::Planner::new()
                    .plan(&pipeline)
                    .map_err(|error| QueryTextError::Planning(error.to_string()))
            });
        match readonly_plan {
            Ok(plan) if plan.is_read_only() => {}
            Ok(_) => reject!(id, "query.read_only_violation", "readOnly query attempted to mutate storage"),
            Err(error) => reject!(id, "query.invalid", error.to_string()),
        }
    }

    let analyzed_access = QueryAccess::analyze(&query).ok();
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
            let scoped_app_collection = execution_context.is_some()
                && !resource.starts_with('_');
            if !scoped_app_collection
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
    // Scoped queries use the blocking executor so the trusted document
    // scope is applied below query syntax. The legacy streaming path remains
    // unchanged for unscoped queries.
    let streaming = if execution_context.is_none() {
        try_write_streaming_request(engine, writer, &request)
    } else {
        Ok(false)
    };
    match streaming {
        Ok(true) => {
            event_engine.publish_global(
                "query.finished",
                serde_json::json!({ "requestId": id }),
            );
        }
        Ok(false) => {
            let response = execute_request_scoped(
                engine,
                request,
                execution_context.as_ref(),
            );
            let ok = response.is_ok();
            write_response(writer, &response)?;
            let event_type = if ok { "query.finished" } else { "query.failed" };
            event_engine.publish_global(
                event_type,
                serde_json::json!({ "requestId": id }),
            );
            if ok {
                if let (Some(context), Some(collection)) =
                    (execution_context.as_ref(), scoped_write_collection.as_ref())
                {
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
