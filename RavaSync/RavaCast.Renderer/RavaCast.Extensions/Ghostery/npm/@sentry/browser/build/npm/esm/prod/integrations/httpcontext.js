import { safeSetSpanJSONAttributes } from "../../../../../../core/build/esm/tracing/spans/captureSpan.js";
import { defineIntegration } from "../../../../../../core/build/esm/integration.js";
import { WINDOW, getHttpRequestData } from "../helpers.js";
//#region node_modules/@sentry/browser/build/npm/esm/prod/integrations/httpcontext.js
/**
* Collects information about HTTP request headers and
* attaches them to the event.
*/
var httpContextIntegration = defineIntegration(() => {
	return {
		name: "HttpContext",
		preprocessEvent(event) {
			if (!WINDOW.navigator && !WINDOW.location && !WINDOW.document) return;
			const reqData = getHttpRequestData();
			const headers = {
				...reqData.headers,
				...event.request?.headers
			};
			event.request = {
				...reqData,
				...event.request,
				headers
			};
		},
		processSegmentSpan(span) {
			if (!WINDOW.navigator && !WINDOW.location && !WINDOW.document) return;
			const reqData = getHttpRequestData();
			safeSetSpanJSONAttributes(span, {
				"url.full": reqData.url || void 0,
				"http.request.header.user_agent": reqData.headers["User-Agent"],
				"http.request.header.referer": reqData.headers["Referer"]
			});
		}
	};
});
//#endregion
export { httpContextIntegration };
