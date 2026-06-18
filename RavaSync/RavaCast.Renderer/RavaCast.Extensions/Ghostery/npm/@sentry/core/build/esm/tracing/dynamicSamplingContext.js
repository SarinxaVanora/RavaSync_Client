import { getClient } from "../currentScopes.js";
import "../semanticAttributes.js";
import { getCapturedScopesOnSpan } from "./utils.js";
import { baggageHeaderToDynamicSamplingContext } from "../utils/baggage.js";
import { extractOrgIdFromClient } from "../utils/dsn.js";
import { getRootSpan, spanIsSampled, spanToJSON } from "../utils/spanUtils.js";
import { hasSpansEnabled } from "../utils/hasSpansEnabled.js";
import "../constants.js";
//#region node_modules/@sentry/core/build/esm/tracing/dynamicSamplingContext.js
/**
* If you change this value, also update the terser plugin config to
* avoid minification of the object property!
*/
var FROZEN_DSC_FIELD = "_frozenDsc";
/**
* Creates a dynamic sampling context from a client.
*
* Dispatches the `createDsc` lifecycle hook as a side effect.
*/
function getDynamicSamplingContextFromClient(trace_id, client) {
	const options = client.getOptions();
	const { publicKey: public_key } = client.getDsn() || {};
	const dsc = {
		environment: options.environment || "production",
		release: options.release,
		public_key,
		trace_id,
		org_id: extractOrgIdFromClient(client)
	};
	client.emit("createDsc", dsc);
	return dsc;
}
/**
* Get the dynamic sampling context for the currently active scopes.
*/
function getDynamicSamplingContextFromScope(client, scope) {
	const propagationContext = scope.getPropagationContext();
	return propagationContext.dsc || getDynamicSamplingContextFromClient(propagationContext.traceId, client);
}
/**
* Creates a dynamic sampling context from a span (and client and scope)
*
* @param span the span from which a few values like the root span name and sample rate are extracted.
*
* @returns a dynamic sampling context
*/
function getDynamicSamplingContextFromSpan(span) {
	const client = getClient();
	if (!client) return {};
	const rootSpan = getRootSpan(span);
	const rootSpanJson = spanToJSON(rootSpan);
	const rootSpanAttributes = rootSpanJson.data;
	const traceState = rootSpan.spanContext().traceState;
	const rootSpanSampleRate = traceState?.get("sentry.sample_rate") ?? rootSpanAttributes["sentry.sample_rate"] ?? rootSpanAttributes["sentry.previous_trace_sample_rate"];
	function applyLocalSampleRateToDsc(dsc) {
		if (typeof rootSpanSampleRate === "number" || typeof rootSpanSampleRate === "string") dsc.sample_rate = `${rootSpanSampleRate}`;
		return dsc;
	}
	const frozenDsc = rootSpan[FROZEN_DSC_FIELD];
	if (frozenDsc) return applyLocalSampleRateToDsc(frozenDsc);
	const traceStateDsc = traceState?.get("sentry.dsc");
	const dscOnTraceState = traceStateDsc && baggageHeaderToDynamicSamplingContext(traceStateDsc);
	if (dscOnTraceState) return applyLocalSampleRateToDsc(dscOnTraceState);
	const dsc = getDynamicSamplingContextFromClient(span.spanContext().traceId, client);
	const source = rootSpanAttributes["sentry.source"] ?? rootSpanAttributes["sentry.span.source"];
	const name = rootSpanJson.description;
	if (source !== "url" && name) dsc.transaction = name;
	if (hasSpansEnabled()) {
		dsc.sampled = String(spanIsSampled(rootSpan));
		dsc.sample_rand = traceState?.get("sentry.sample_rand") ?? getCapturedScopesOnSpan(rootSpan).scope?.getPropagationContext().sampleRand.toString();
	}
	applyLocalSampleRateToDsc(dsc);
	client.emit("createDsc", dsc, rootSpan);
	return dsc;
}
//#endregion
export { getDynamicSamplingContextFromScope, getDynamicSamplingContextFromSpan };
