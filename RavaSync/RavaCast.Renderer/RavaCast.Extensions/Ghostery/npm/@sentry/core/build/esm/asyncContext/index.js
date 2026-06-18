import { getSentryCarrier } from "../carrier.js";
import { getStackAsyncContextStrategy } from "./stackStrategy.js";
//#region node_modules/@sentry/core/build/esm/asyncContext/index.js
/**
* Get the current async context strategy.
* If none has been setup, the default will be used.
*/
function getAsyncContextStrategy(carrier) {
	const sentry = getSentryCarrier(carrier);
	if (sentry.acs) return sentry.acs;
	return getStackAsyncContextStrategy();
}
//#endregion
export { getAsyncContextStrategy };
