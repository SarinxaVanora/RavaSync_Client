//#region node_modules/@adguard/agtree/dist/errors/adblock-syntax-error.js
/**
* @file Customized syntax error class for Adblock Filter Parser.
*/
var ERROR_NAME = "AdblockSyntaxError";
/**
* Customized syntax error class for Adblock Filter Parser,
* which contains the location range of the error.
*/
var AdblockSyntaxError = class extends SyntaxError {
	/**
	* Start offset of the error.
	*/
	start;
	/**
	* End offset of the error.
	*/
	end;
	/**
	* Constructs a new `AdblockSyntaxError` instance.
	*
	* @param message Error message.
	* @param start Start offset of the error.
	* @param end End offset of the error.
	*/
	constructor(message, start, end) {
		super(message);
		this.name = ERROR_NAME;
		this.start = start;
		this.end = end;
	}
};
//#endregion
export { AdblockSyntaxError };
