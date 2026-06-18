import { safeSetSpanJSONAttributes } from "../../../../../../core/build/esm/tracing/spans/captureSpan.js";
import { defineIntegration } from "../../../../../../core/build/esm/integration.js";
import { WINDOW } from "../helpers.js";
//#region node_modules/@sentry/browser/build/npm/esm/prod/integrations/culturecontext.js
var INTEGRATION_NAME = "CultureContext";
var _cultureContextIntegration = (() => {
	return {
		name: INTEGRATION_NAME,
		preprocessEvent(event) {
			const culture = getCultureContext();
			if (culture) event.contexts = {
				...event.contexts,
				culture: {
					...culture,
					...event.contexts?.culture
				}
			};
		},
		processSegmentSpan(span) {
			const culture = getCultureContext();
			if (culture) safeSetSpanJSONAttributes(span, {
				"culture.locale": culture.locale,
				"culture.timezone": culture.timezone,
				"culture.calendar": culture.calendar
			});
		}
	};
});
/**
* Captures culture context from the browser.
*
* Enabled by default.
*
* @example
* ```js
* import * as Sentry from '@sentry/browser';
*
* Sentry.init({
*   integrations: [Sentry.cultureContextIntegration()],
* });
* ```
*/
var cultureContextIntegration = defineIntegration(_cultureContextIntegration);
/**
* Returns the culture context from the browser's Intl API.
*/
function getCultureContext() {
	try {
		const intl = WINDOW.Intl;
		if (!intl) return;
		const options = intl.DateTimeFormat().resolvedOptions();
		return {
			locale: options.locale,
			timezone: options.timeZone,
			calendar: options.calendar
		};
	} catch {
		return;
	}
}
//#endregion
export { cultureContextIntegration };
