import { DEBUG_BUILD } from "./debug-build.js";
import { consoleSandbox, debug } from "./utils/debug-logger.js";
import { getCurrentScope } from "./currentScopes.js";
//#region node_modules/@sentry/core/build/esm/sdk.js
/** A class object that can instantiate Client objects. */
/**
* Internal function to create a new SDK client instance. The client is
* installed and then bound to the current scope.
*
* @param clientClass The client class to instantiate.
* @param options Options to pass to the client.
*/
function initAndBind(clientClass, options) {
	if (options.debug === true) if (DEBUG_BUILD) debug.enable();
	else consoleSandbox(() => {
		console.warn("[Sentry] Cannot initialize SDK with `debug` option using a non-debug bundle.");
	});
	getCurrentScope().update(options.initialScope);
	const client = new clientClass(options);
	setCurrentClient(client);
	client.init();
	return client;
}
/**
* Make the given client the current client.
*/
function setCurrentClient(client) {
	getCurrentScope().setClient(client);
}
//#endregion
export { initAndBind };
