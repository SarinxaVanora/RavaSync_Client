//#region node_modules/@adguard/agtree/dist/errors/not-implemented-error.js
/**
* @file Customized error class for not implemented features.
*/
var ERROR_NAME = "NotImplementedError";
var BASE_MESSAGE = "Not implemented";
/**
* Customized error class for not implemented features.
*/
var NotImplementedError = class extends Error {
	/**
	* Constructs a new `NotImplementedError` instance.
	*
	* @param message Additional error message (optional)
	*/
	constructor(message = void 0) {
		const fullMessage = message ? `${BASE_MESSAGE}: ${message}` : BASE_MESSAGE;
		super(fullMessage);
		this.name = ERROR_NAME;
	}
};
//#endregion
export { NotImplementedError };
