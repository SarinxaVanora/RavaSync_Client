import { debug } from "../../../../../../core/build/esm/utils/debug-logger.js";
import { getIsolationScope } from "../../../../../../core/build/esm/currentScopes.js";
import { captureSession, startSession } from "../../../../../../core/build/esm/exports.js";
import { defineIntegration } from "../../../../../../core/build/esm/integration.js";
import { WINDOW } from "../helpers.js";
import { addHistoryInstrumentationHandler } from "../../../../../../../@sentry-internal/browser-utils/build/esm/instrument/history.js";
import { DEBUG_BUILD } from "../debug-build.js";
//#region node_modules/@sentry/browser/build/npm/esm/prod/integrations/browsersession.js
/**
* When added, automatically creates sessions which allow you to track adoption and crashes (crash free rate) in your Releases in Sentry.
* More information: https://docs.sentry.io/product/releases/health/
*
* Note: In order for session tracking to work, you need to set up Releases: https://docs.sentry.io/product/releases/
*/
var browserSessionIntegration = defineIntegration((options = {}) => {
	const lifecycle = options.lifecycle ?? "route";
	return {
		name: "BrowserSession",
		setupOnce() {
			if (typeof WINDOW.document === "undefined") {
				DEBUG_BUILD && debug.warn("Using the `browserSessionIntegration` in non-browser environments is not supported.");
				return;
			}
			startSession({ ignoreDuration: true });
			captureSession();
			const isolationScope = getIsolationScope();
			let previousUser = isolationScope.getUser();
			isolationScope.addScopeListener((scope) => {
				const maybeNewUser = scope.getUser();
				if (previousUser?.id !== maybeNewUser?.id || previousUser?.ip_address !== maybeNewUser?.ip_address) {
					captureSession();
					previousUser = maybeNewUser;
				}
			});
			if (lifecycle === "route") addHistoryInstrumentationHandler(({ from, to }) => {
				if (from !== to) {
					startSession({ ignoreDuration: true });
					captureSession();
				}
			});
		}
	};
});
//#endregion
export { browserSessionIntegration };
