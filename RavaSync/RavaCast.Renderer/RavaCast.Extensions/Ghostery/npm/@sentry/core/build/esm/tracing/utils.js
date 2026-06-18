import { derefWeakRef } from "../utils/weakRef.js";
//#region node_modules/@sentry/core/build/esm/tracing/utils.js
var SCOPE_ON_START_SPAN_FIELD = "_sentryScope";
var ISOLATION_SCOPE_ON_START_SPAN_FIELD = "_sentryIsolationScope";
/**
* Grabs the scope and isolation scope off a span that were active when the span was started.
* If WeakRef was used and scopes have been garbage collected, returns undefined for those scopes.
*/
function getCapturedScopesOnSpan(span) {
	const spanWithScopes = span;
	return {
		scope: spanWithScopes[SCOPE_ON_START_SPAN_FIELD],
		isolationScope: derefWeakRef(spanWithScopes[ISOLATION_SCOPE_ON_START_SPAN_FIELD])
	};
}
//#endregion
export { getCapturedScopesOnSpan };
