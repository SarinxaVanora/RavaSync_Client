//#region node_modules/@sentry/core/build/esm/semanticAttributes.js
/**
* Use this attribute to represent the source of a span name.
* Must be one of: custom, url, route, view, component, task
* TODO(v11): rename this to sentry.span.source'
*/
var SEMANTIC_ATTRIBUTE_SENTRY_SOURCE = "sentry.source";
/**
* Attributes that holds the sample rate that was locally applied to a span.
* If this attribute is not defined, it means that the span inherited a sampling decision.
*
* NOTE: Is only defined on root spans.
*/
var SEMANTIC_ATTRIBUTE_SENTRY_SAMPLE_RATE = "sentry.sample_rate";
/**
* Attribute holding the sample rate of the previous trace.
* This is used to sample consistently across subsequent traces in the browser SDK.
*
* Note: Only defined on root spans, if opted into consistent sampling
*/
var SEMANTIC_ATTRIBUTE_SENTRY_PREVIOUS_TRACE_SAMPLE_RATE = "sentry.previous_trace_sample_rate";
/**
* Use this attribute to represent the operation of a span.
*/
var SEMANTIC_ATTRIBUTE_SENTRY_OP = "sentry.op";
/**
* Use this attribute to represent the origin of a span.
*/
var SEMANTIC_ATTRIBUTE_SENTRY_ORIGIN = "sentry.origin";
/**
* The id of the profile that this span occurred in.
*/
var SEMANTIC_ATTRIBUTE_PROFILE_ID = "sentry.profile_id";
var SEMANTIC_ATTRIBUTE_EXCLUSIVE_TIME = "sentry.exclusive_time";
/**
* =============================================================================
* GEN AI ATTRIBUTES
* Based on OpenTelemetry Semantic Conventions for Generative AI
* @see https://opentelemetry.io/docs/specs/semconv/gen-ai/
* =============================================================================
*/
/**
* The conversation ID for linking messages across API calls.
* For OpenAI Assistants API: thread_id
* For LangGraph: configurable.thread_id
*/
var GEN_AI_CONVERSATION_ID_ATTRIBUTE = "gen_ai.conversation.id";
//#endregion
export { GEN_AI_CONVERSATION_ID_ATTRIBUTE, SEMANTIC_ATTRIBUTE_EXCLUSIVE_TIME, SEMANTIC_ATTRIBUTE_PROFILE_ID, SEMANTIC_ATTRIBUTE_SENTRY_OP, SEMANTIC_ATTRIBUTE_SENTRY_ORIGIN, SEMANTIC_ATTRIBUTE_SENTRY_PREVIOUS_TRACE_SAMPLE_RATE, SEMANTIC_ATTRIBUTE_SENTRY_SAMPLE_RATE, SEMANTIC_ATTRIBUTE_SENTRY_SOURCE };
