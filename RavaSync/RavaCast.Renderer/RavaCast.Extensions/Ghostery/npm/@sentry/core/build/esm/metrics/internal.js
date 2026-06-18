import { getGlobalSingleton } from "../carrier.js";
import { createMetricEnvelope } from "./envelope.js";
//#region node_modules/@sentry/core/build/esm/metrics/internal.js
/**
* Flushes the metrics buffer to Sentry.
*
* @param client - A client.
* @param maybeMetricBuffer - A metric buffer. Uses the metric buffer for the given client if not provided.
*
* @experimental This method will experience breaking changes. This is not yet part of
* the stable Sentry SDK API and can be changed or removed without warning.
*/
function _INTERNAL_flushMetricsBuffer(client, maybeMetricBuffer) {
	const metricBuffer = maybeMetricBuffer ?? _INTERNAL_getMetricBuffer(client) ?? [];
	if (metricBuffer.length === 0) return;
	const clientOptions = client.getOptions();
	const envelope = createMetricEnvelope(metricBuffer, clientOptions._metadata, clientOptions.tunnel, client.getDsn(), clientOptions.sendDefaultPii);
	_getBufferMap().set(client, []);
	client.emit("flushMetrics");
	client.sendEnvelope(envelope);
}
/**
* Returns the metric buffer for a given client.
*
* Exported for testing purposes.
*
* @param client - The client to get the metric buffer for.
* @returns The metric buffer for the given client.
*/
function _INTERNAL_getMetricBuffer(client) {
	return _getBufferMap().get(client);
}
function _getBufferMap() {
	return getGlobalSingleton("clientToMetricBufferMap", () => /* @__PURE__ */ new WeakMap());
}
//#endregion
export { _INTERNAL_flushMetricsBuffer };
