import { AdblockSyntaxError } from "../../../../errors/adblock-syntax-error.js";
import { require_sprintf } from "../../../../../../../sprintf-js/src/sprintf.js";
import { getFormattedTokenName } from "../../../../../../css-tokenizer/dist/csstokenizer.js";
import { CompoundSelectorHandler } from "./compound-selector-handler.js";
//#region node_modules/@adguard/agtree/dist/parser/cosmetic/selector/handlers/complex-selector-handler.js
var import_sprintf = require_sprintf();
/**
* Handles complex selector parsing in selector list.
*/
var ComplexSelectorHandler = class {
	/**
	* Finishes the current complex selector by:
	* 1. Finishing current compound selector node via {@link CompoundSelectorHandler},
	* 2. Validating current complex selector node,
	* 3. Appending current complex selector node to the selector list node,
	* If `isEof` is `false`:
	* 4. Constructing next complex selector node.
	*
	* @param context Selector list parser context.
	* @param isEof Indicates whether the end of the file has been reached.
	*
	* @throws If the current compound / complex selector has no simple selectors / compound selectors.
	*/
	static handle(context, isEof = true) {
		const { raw, options, baseOffset, stream, token, result, complexSelector } = context;
		const currentEndToken = stream.lookbehindForNonWs();
		CompoundSelectorHandler.handle(context);
		if (!currentEndToken || complexSelector.children.length === 0) throw new AdblockSyntaxError((0, import_sprintf.sprintf)("Unexpected token '%s' with value '%s'", getFormattedTokenName(token.type), raw.slice(token.start, token.end)), baseOffset + token.start, baseOffset + token.end);
		if (options.isLocIncluded) complexSelector.end = baseOffset + currentEndToken.end;
		result.children.push(complexSelector);
		if (isEof) return;
		const nextStartToken = stream.getOrFail();
		const nextComplexSelector = {
			type: "ComplexSelector",
			children: []
		};
		if (options.isLocIncluded) nextComplexSelector.start = baseOffset + nextStartToken.start;
		context.complexSelector = nextComplexSelector;
		context.isTypeSelectorSet = false;
	}
};
//#endregion
export { ComplexSelectorHandler };
