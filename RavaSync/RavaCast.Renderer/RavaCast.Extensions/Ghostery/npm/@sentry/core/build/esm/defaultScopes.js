import { getGlobalSingleton } from "./carrier.js";
import { Scope } from "./scope.js";
//#region node_modules/@sentry/core/build/esm/defaultScopes.js
/** Get the default current scope. */
function getDefaultCurrentScope() {
	return getGlobalSingleton("defaultCurrentScope", () => new Scope());
}
/** Get the default isolation scope. */
function getDefaultIsolationScope() {
	return getGlobalSingleton("defaultIsolationScope", () => new Scope());
}
//#endregion
export { getDefaultCurrentScope, getDefaultIsolationScope };
