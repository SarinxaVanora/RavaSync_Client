import { stackParserFromStackParserOptions } from "../../../../../core/build/esm/utils/stacktrace.js";
import { getIntegrationsToSetup } from "../../../../../core/build/esm/integration.js";
import { initAndBind } from "../../../../../core/build/esm/sdk.js";
import { functionToStringIntegration } from "../../../../../core/build/esm/integrations/functiontostring.js";
import { inboundFiltersIntegration } from "../../../../../core/build/esm/integrations/eventFilters.js";
import { dedupeIntegration } from "../../../../../core/build/esm/integrations/dedupe.js";
import { conversationIdIntegration } from "../../../../../core/build/esm/integrations/conversationId.js";
import { BrowserClient } from "./client.js";
import { makeFetchTransport } from "./transports/fetch.js";
import { defaultStackParser } from "./stack-parsers.js";
import { breadcrumbsIntegration } from "./integrations/breadcrumbs.js";
import { browserApiErrorsIntegration } from "./integrations/browserapierrors.js";
import { browserSessionIntegration } from "./integrations/browsersession.js";
import { cultureContextIntegration } from "./integrations/culturecontext.js";
import { globalHandlersIntegration } from "./integrations/globalhandlers.js";
import { httpContextIntegration } from "./integrations/httpcontext.js";
import { linkedErrorsIntegration } from "./integrations/linkederrors.js";
import { checkAndWarnIfIsEmbeddedBrowserExtension } from "./utils/detectBrowserExtension.js";
//#region node_modules/@sentry/browser/build/npm/esm/prod/sdk.js
/** Get the default integrations for the browser SDK. */
function getDefaultIntegrations(_options) {
	/**
	* Note: Please make sure this stays in sync with Angular SDK, which re-exports
	* `getDefaultIntegrations` but with an adjusted set of integrations.
	*/
	return [
		inboundFiltersIntegration(),
		functionToStringIntegration(),
		conversationIdIntegration(),
		browserApiErrorsIntegration(),
		breadcrumbsIntegration(),
		globalHandlersIntegration(),
		linkedErrorsIntegration(),
		dedupeIntegration(),
		httpContextIntegration(),
		cultureContextIntegration(),
		browserSessionIntegration()
	];
}
/**
* The Sentry Browser SDK Client.
*
* To use this SDK, call the {@link init} function as early as possible when
* loading the web page. To set context information or send manual events, use
* the provided methods.
*
* @example
*
* ```
*
* import { init } from '@sentry/browser';
*
* init({
*   dsn: '__DSN__',
*   // ...
* });
* ```
*
* @example
* ```
*
* import { addBreadcrumb } from '@sentry/browser';
* addBreadcrumb({
*   message: 'My Breadcrumb',
*   // ...
* });
* ```
*
* @example
*
* ```
*
* import * as Sentry from '@sentry/browser';
* Sentry.captureMessage('Hello, world!');
* Sentry.captureException(new Error('Good bye'));
* Sentry.captureEvent({
*   message: 'Manual',
*   stacktrace: [
*     // ...
*   ],
* });
* ```
*
* @see {@link BrowserOptions} for documentation on configuration options.
*/
function init(options = {}) {
	const shouldDisableBecauseIsBrowserExtenstion = !options.skipBrowserExtensionCheck && checkAndWarnIfIsEmbeddedBrowserExtension();
	let defaultIntegrations = options.defaultIntegrations == null ? getDefaultIntegrations() : options.defaultIntegrations;
	return initAndBind(BrowserClient, {
		...options,
		enabled: shouldDisableBecauseIsBrowserExtenstion ? false : options.enabled,
		stackParser: stackParserFromStackParserOptions(options.stackParser || defaultStackParser),
		integrations: getIntegrationsToSetup({
			integrations: options.integrations,
			defaultIntegrations
		}),
		transport: options.transport || makeFetchTransport
	});
}
//#endregion
export { init };
