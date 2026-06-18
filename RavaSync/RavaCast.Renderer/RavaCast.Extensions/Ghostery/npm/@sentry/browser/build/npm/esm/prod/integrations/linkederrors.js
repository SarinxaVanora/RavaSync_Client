import { defineIntegration } from "../../../../../../core/build/esm/integration.js";
import { applyAggregateErrorsToEvent } from "../../../../../../core/build/esm/utils/aggregate-errors.js";
import { exceptionFromError } from "../eventbuilder.js";
//#region node_modules/@sentry/browser/build/npm/esm/prod/integrations/linkederrors.js
var DEFAULT_KEY = "cause";
var DEFAULT_LIMIT = 5;
var INTEGRATION_NAME = "LinkedErrors";
var _linkedErrorsIntegration = ((options = {}) => {
	const limit = options.limit || DEFAULT_LIMIT;
	const key = options.key || DEFAULT_KEY;
	return {
		name: INTEGRATION_NAME,
		preprocessEvent(event, hint, client) {
			applyAggregateErrorsToEvent(exceptionFromError, client.getOptions().stackParser, key, limit, event, hint);
		}
	};
});
/**
* Aggregrate linked errors in an event.
*/
var linkedErrorsIntegration = defineIntegration(_linkedErrorsIntegration);
//#endregion
export { linkedErrorsIntegration };
