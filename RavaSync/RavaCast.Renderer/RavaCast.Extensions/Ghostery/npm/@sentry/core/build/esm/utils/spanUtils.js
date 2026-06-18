import { consoleSandbox } from "./debug-logger.js";
import { timestampInSeconds } from "./time.js";
import { generateSpanId } from "./propagationContext.js";
import { serializeAttributes } from "../attributes.js";
import { SEMANTIC_ATTRIBUTE_SENTRY_OP, SEMANTIC_ATTRIBUTE_SENTRY_ORIGIN } from "../semanticAttributes.js";
import { getCapturedScopesOnSpan } from "../tracing/utils.js";
var hasShownSpanDropWarning = false;
/**
* Convert a span to a trace context, which can be sent as the `trace` context in a non-transaction event.
*/
function spanToTraceContext(span) {
	const { spanId, traceId: trace_id, isRemote } = span.spanContext();
	const parent_span_id = isRemote ? spanId : spanToJSON(span).parent_span_id;
	const scope = getCapturedScopesOnSpan(span).scope;
	return {
		parent_span_id,
		span_id: isRemote ? scope?.getPropagationContext().propagationSpanId || generateSpanId() : spanId,
		trace_id
	};
}
/**
*  Converts the span links array to a flattened version to be sent within an envelope.
*
*  If the links array is empty, it returns `undefined` so the empty value can be dropped before it's sent.
*/
function convertSpanLinksForEnvelope(links) {
	if (links && links.length > 0) return links.map(({ context: { spanId, traceId, traceFlags, ...restContext }, attributes }) => ({
		span_id: spanId,
		trace_id: traceId,
		sampled: traceFlags === 1,
		attributes,
		...restContext
	}));
	else return;
}
/**
* Convert a span time input into a timestamp in seconds.
*/
function spanTimeInputToSeconds(input) {
	if (typeof input === "number") return ensureTimestampInSeconds(input);
	if (Array.isArray(input)) return input[0] + input[1] / 1e9;
	if (input instanceof Date) return ensureTimestampInSeconds(input.getTime());
	return timestampInSeconds();
}
/**
* Converts a timestamp to second, if it was in milliseconds, or keeps it as second.
*/
function ensureTimestampInSeconds(timestamp) {
	return timestamp > 9999999999 ? timestamp / 1e3 : timestamp;
}
/**
* Convert a span to a JSON representation.
*/
function spanToJSON(span) {
	if (spanIsSentrySpan(span)) return span.getSpanJSON();
	const { spanId: span_id, traceId: trace_id } = span.spanContext();
	if (spanIsOpenTelemetrySdkTraceBaseSpan(span)) {
		const { attributes, startTime, name, endTime, status, links } = span;
		return {
			span_id,
			trace_id,
			data: attributes,
			description: name,
			parent_span_id: getOtelParentSpanId(span),
			start_timestamp: spanTimeInputToSeconds(startTime),
			timestamp: spanTimeInputToSeconds(endTime) || void 0,
			status: getStatusMessage(status),
			op: attributes[SEMANTIC_ATTRIBUTE_SENTRY_OP],
			origin: attributes[SEMANTIC_ATTRIBUTE_SENTRY_ORIGIN],
			links: convertSpanLinksForEnvelope(links)
		};
	}
	return {
		span_id,
		trace_id,
		start_timestamp: 0,
		data: {}
	};
}
/**
* In preparation for the next major of OpenTelemetry, we want to support
* looking up the parent span id according to the new API
* In OTel v1, the parent span id is accessed as `parentSpanId`
* In OTel v2, the parent span id is accessed as `spanId` on the `parentSpanContext`
*/
function getOtelParentSpanId(span) {
	return "parentSpanId" in span ? span.parentSpanId : "parentSpanContext" in span ? span.parentSpanContext?.spanId : void 0;
}
/**
* Converts a {@link StreamedSpanJSON} to a {@link SerializedSpan}.
* This is the final serialized span format that is sent to Sentry.
* The returned serilaized spans must not be consumed by users or SDK integrations.
*/
function streamedSpanJsonToSerializedSpan(spanJson) {
	return {
		...spanJson,
		attributes: serializeAttributes(spanJson.attributes),
		links: spanJson.links?.map((link) => ({
			...link,
			attributes: serializeAttributes(link.attributes)
		}))
	};
}
function spanIsOpenTelemetrySdkTraceBaseSpan(span) {
	const castSpan = span;
	return !!castSpan.attributes && !!castSpan.startTime && !!castSpan.name && !!castSpan.endTime && !!castSpan.status;
}
/** Exported only for tests. */
/**
* Sadly, due to circular dependency checks we cannot actually import the Span class here and check for instanceof.
* :( So instead we approximate this by checking if it has the `getSpanJSON` method.
*/
function spanIsSentrySpan(span) {
	return typeof span.getSpanJSON === "function";
}
/**
* Returns true if a span is sampled.
* In most cases, you should just use `span.isRecording()` instead.
* However, this has a slightly different semantic, as it also returns false if the span is finished.
* So in the case where this distinction is important, use this method.
*/
function spanIsSampled(span) {
	const { traceFlags } = span.spanContext();
	return traceFlags === 1;
}
/** Get the status message to use for a JSON representation of a span. */
function getStatusMessage(status) {
	if (!status || status.code === 0) return;
	if (status.code === 1) return "ok";
	return status.message || "internal_error";
}
var ROOT_SPAN_FIELD = "_sentryRootSpan";
/**
* Returns the root span of a given span.
*/
var getRootSpan = INTERNAL_getSegmentSpan;
/**
* Returns the segment span of a given span.
*/
function INTERNAL_getSegmentSpan(span) {
	return span[ROOT_SPAN_FIELD] || span;
}
/**
* Logs a warning once if `beforeSendSpan` is used to drop spans.
*/
function showSpanDropWarning() {
	if (!hasShownSpanDropWarning) {
		consoleSandbox(() => {
			console.warn("[Sentry] Returning null from `beforeSendSpan` is disallowed. To drop certain spans, configure the respective integrations directly or use `ignoreSpans`.");
		});
		hasShownSpanDropWarning = true;
	}
}
//#endregion
export { getRootSpan, showSpanDropWarning, spanIsSampled, spanToJSON, spanToTraceContext, streamedSpanJsonToSerializedSpan };
