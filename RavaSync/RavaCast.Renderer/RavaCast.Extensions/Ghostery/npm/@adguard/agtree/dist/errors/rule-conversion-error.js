//#region node_modules/@adguard/agtree/dist/errors/rule-conversion-error.js
/**
* @file Customized error class for conversion errors.
*/
var ERROR_NAME = "RuleConversionError";
/**
* Customized error class for conversion errors.
*/
var RuleConversionError = class extends Error {
	/**
	* Constructs a new `RuleConversionError` instance.
	*
	* @param message Error message
	*/
	constructor(message) {
		super(message);
		this.name = ERROR_NAME;
	}
};
//#endregion
export { RuleConversionError };
