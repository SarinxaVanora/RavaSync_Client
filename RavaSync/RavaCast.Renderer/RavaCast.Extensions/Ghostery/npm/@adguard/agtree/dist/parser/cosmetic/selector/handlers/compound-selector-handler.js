import { AdblockSyntaxError } from "../../../../errors/adblock-syntax-error.js";
import { require_sprintf } from "../../../../../../../sprintf-js/src/sprintf.js";
import { getFormattedTokenName } from "../../../../../../css-tokenizer/dist/csstokenizer.js";
//#region node_modules/@adguard/agtree/dist/parser/cosmetic/selector/handlers/compound-selector-handler.js
var import_sprintf = require_sprintf();
/**
* Handles compound selector parsing in selector list.
*/
var CompoundSelectorHandler = class CompoundSelectorHandler {
	/**
	* Set of allowed symbols between selectors (combinators + comma, except <space> combinator).
	*
	* @see {@link SelectorCombinatorValue}
	*/
	static ALLOWED_SYMBOLS_BETWEEN_SELECTORS = new Set([
		">",
		"+",
		"~",
		","
	]);
	/**
	* Finishes the current compound selector by:
	* 1. Validating current compound selector node,
	* 2. Constructing selector combinator node (if provided),
	* 3. Appending selector combinator node to the complex selector (if provided).
	*
	* @param context Selector list parser context.
	* @param combinator Optional combinator string.
	*
	* @throws If the current compound selector has no simple selectors.
	*/
	static handle(context, combinator) {
		const { raw, options, baseOffset, stream, token, complexSelector } = context;
		if (!stream.lookbehindForNonWs() || complexSelector.children.length === 0 || complexSelector.children[complexSelector.children.length - 1].type === "SelectorCombinator") throw new AdblockSyntaxError((0, import_sprintf.sprintf)("Unexpected token '%s' with value '%s'", getFormattedTokenName(token.type), raw.slice(token.start, token.end)), baseOffset + token.start, baseOffset + token.end);
		if (combinator === " ") {
			stream.skipWhitespace();
			if (!stream.get()) return;
			if (CompoundSelectorHandler.ALLOWED_SYMBOLS_BETWEEN_SELECTORS.has(stream.fragment())) return;
		} else {
			stream.advance();
			stream.skipWhitespace();
		}
		if (!combinator) return;
		stream.getOrFail();
		const result = {
			type: "SelectorCombinator",
			value: combinator
		};
		if (options.isLocIncluded) {
			result.start = baseOffset + token.start;
			result.end = baseOffset + token.start + combinator.length;
		}
		complexSelector.children.push(result);
		context.isTypeSelectorSet = false;
	}
};
//#endregion
export { CompoundSelectorHandler };
