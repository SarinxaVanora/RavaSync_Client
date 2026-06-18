import { streamedSpanJsonToSerializedSpan } from "../../utils/spanUtils.js";
//#region node_modules/@sentry/core/build/esm/tracing/spans/spanJsonToStreamedSpan.js
/**
* Converts a v1 SpanJSON (from a legacy transaction) to a serialized v2 StreamedSpan.
*/
function spanJsonToSerializedStreamedSpan(span) {
	return streamedSpanJsonToSerializedSpan({
		trace_id: span.trace_id,
		span_id: span.span_id,
		parent_span_id: span.parent_span_id,
		name: span.description || "",
		start_timestamp: span.start_timestamp,
		end_timestamp: span.timestamp || span.start_timestamp,
		status: !span.status || span.status === "ok" || span.status === "cancelled" ? "ok" : "error",
		is_segment: false,
		attributes: { ...span.data },
		links: span.links
	});
}
//#endregion
export { spanJsonToSerializedStreamedSpan };
