import { NotImplementedError } from "../../errors/not-implemented-error.js";
import { BaseConverter } from "./base-converter.js";
//#region node_modules/@adguard/agtree/dist/converter/base-interfaces/rule-converter-base.js
/**
* @file Base class for rule converters
*
* TS doesn't support abstract static methods, so we should use
* a workaround and extend this class instead of implementing it
*/
/**
* Basic class for rule converters
*/
var RuleConverterBase = class extends BaseConverter {
	/**
	* Converts an adblock filtering rule to AdGuard format, if possible.
	*
	* @param rule Rule node to convert
	* @returns An object which follows the {@link NodeConversionResult} interface. Its `result` property contains
	* the array of converted rule nodes, and its `isConverted` flag indicates whether the original rule was converted.
	* If the rule was not converted, the result array will contain the original node with the same object reference
	* @throws If the rule is invalid or cannot be converted
	*/
	static convertToAdg(rule) {
		throw new NotImplementedError();
	}
	/**
	* Converts an adblock filtering rule to Adblock Plus format, if possible.
	*
	* @param rule Rule node to convert
	* @returns An object which follows the {@link NodeConversionResult} interface. Its `result` property contains
	* the array of converted rule nodes, and its `isConverted` flag indicates whether the original rule was converted.
	* If the rule was not converted, the result array will contain the original node with the same object reference
	* @throws If the rule is invalid or cannot be converted
	*/
	static convertToAbp(rule) {
		throw new NotImplementedError();
	}
	/**
	* Converts an adblock filtering rule to uBlock Origin format, if possible.
	*
	* @param rule Rule node to convert
	* @returns An object which follows the {@link NodeConversionResult} interface. Its `result` property contains
	* the array of converted rule nodes, and its `isConverted` flag indicates whether the original rule was converted.
	* If the rule was not converted, the result array will contain the original node with the same object reference
	* @throws If the rule is invalid or cannot be converted
	*/
	static convertToUbo(rule) {
		throw new NotImplementedError();
	}
};
//#endregion
export { RuleConverterBase };
