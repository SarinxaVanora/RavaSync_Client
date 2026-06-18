import { getSDKSource } from "../../../../../core/build/esm/utils/env.js";
import { _INTERNAL_flushLogsBuffer } from "../../../../../core/build/esm/logs/internal.js";
import { _INTERNAL_flushMetricsBuffer } from "../../../../../core/build/esm/metrics/internal.js";
import { Client } from "../../../../../core/build/esm/client.js";
import { addAutoIpAddressToSession } from "../../../../../core/build/esm/utils/ipAddress.js";
import { applySdkMetadata } from "../../../../../core/build/esm/utils/sdkMetadata.js";
import { WINDOW } from "./helpers.js";
import { eventFromException, eventFromMessage } from "./eventbuilder.js";
//#region node_modules/@sentry/browser/build/npm/esm/prod/client.js
/**
* A magic string that build tooling can leverage in order to inject a release value into the SDK.
*/
/**
* The Sentry Browser SDK Client.
*
* @see BrowserOptions for documentation on configuration options.
* @see SentryClient for usage documentation.
*/
var BrowserClient = class extends Client {
	/**
	* Creates a new Browser SDK instance.
	*
	* @param options Configuration options for this SDK.
	*/
	constructor(options) {
		const opts = applyDefaultOptions(options);
		applySdkMetadata(opts, "browser", ["browser"], WINDOW.SENTRY_SDK_SOURCE || getSDKSource());
		if (opts._metadata?.sdk) opts._metadata.sdk.settings = {
			infer_ip: opts.sendDefaultPii ? "auto" : "never",
			...opts._metadata.sdk.settings
		};
		super(opts);
		const { sendDefaultPii, sendClientReports, enableLogs, _experiments, enableMetrics: enableMetricsOption } = this._options;
		const enableMetrics = enableMetricsOption ?? _experiments?.enableMetrics ?? true;
		if (WINDOW.document && (sendClientReports || enableLogs || enableMetrics)) WINDOW.document.addEventListener("visibilitychange", () => {
			if (WINDOW.document.visibilityState === "hidden") {
				if (sendClientReports) this._flushOutcomes();
				if (enableLogs) _INTERNAL_flushLogsBuffer(this);
				if (enableMetrics) _INTERNAL_flushMetricsBuffer(this);
			}
		});
		if (sendDefaultPii) this.on("beforeSendSession", addAutoIpAddressToSession);
	}
	/**
	* @inheritDoc
	*/
	eventFromException(exception, hint) {
		return eventFromException(this._options.stackParser, exception, hint, this._options.attachStacktrace);
	}
	/**
	* @inheritDoc
	*/
	eventFromMessage(message, level = "info", hint) {
		return eventFromMessage(this._options.stackParser, message, level, hint, this._options.attachStacktrace);
	}
	/**
	* @inheritDoc
	*/
	_prepareEvent(event, hint, currentScope, isolationScope) {
		event.platform = event.platform || "javascript";
		return super._prepareEvent(event, hint, currentScope, isolationScope);
	}
};
/** Exported only for tests. */
function applyDefaultOptions(optionsArg) {
	return {
		release: typeof __SENTRY_RELEASE__ === "string" ? __SENTRY_RELEASE__ : WINDOW.SENTRY_RELEASE?.id,
		sendClientReports: true,
		parentSpanIsAlwaysRootSpan: true,
		...optionsArg
	};
}
//#endregion
export { BrowserClient };
