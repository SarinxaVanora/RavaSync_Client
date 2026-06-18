import { hasSpanStreamingEnabled } from "./hasSpanStreamingEnabled.js";
import { isBrowser } from "../../utils/isBrowser.js";
import { spanJsonToSerializedStreamedSpan } from "./spanJsonToStreamedSpan.js";
//#region node_modules/@sentry/core/build/esm/tracing/spans/extractGenAiSpans.js
/**
* Extracts gen_ai spans from a transaction event, converts them to span v2 format,
* and returns them as a SpanContainerItem.
*
* Only applies to static mode (non-streaming) transactions.
*
* WARNING: This function mutates `event.spans` by removing the extracted gen_ai spans
* from the array. Call this before creating the event envelope so the transaction
* item does not include the extracted spans.
*/
function extractGenAiSpansFromEvent(event, client) {
	if (event.type !== "transaction" || !event.spans?.length || !event.sdkProcessingMetadata?.hasGenAiSpans || !client.getOptions().streamGenAiSpans || hasSpanStreamingEnabled(client)) return;
	const genAiSpans = [];
	const remainingSpans = [];
	for (const span of event.spans) if (span.op?.startsWith("gen_ai.")) genAiSpans.push(spanJsonToSerializedStreamedSpan(span));
	else remainingSpans.push(span);
	if (genAiSpans.length === 0) return;
	event.spans = remainingSpans;
	const inferSetting = client.getOptions().sendDefaultPii ? "auto" : "never";
	return [{
		type: "span",
		item_count: genAiSpans.length,
		content_type: "application/vnd.sentry.items.span.v2+json"
	}, {
		version: 2,
		...isBrowser() && { ingest_settings: {
			infer_ip: inferSetting,
			infer_user_agent: inferSetting
		} },
		items: genAiSpans
	}];
}
//#endregion
export { extractGenAiSpansFromEvent };
