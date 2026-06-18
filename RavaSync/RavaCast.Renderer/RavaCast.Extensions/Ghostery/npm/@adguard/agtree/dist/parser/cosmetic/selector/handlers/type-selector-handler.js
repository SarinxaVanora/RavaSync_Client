import { AdblockSyntaxError } from "../../../../errors/adblock-syntax-error.js";
//#region node_modules/@adguard/agtree/dist/parser/cosmetic/selector/handlers/type-selector-handler.js
/**
* Handles type selector parsing in selector list.
*/
var TypeSelectorHandler = class {
	/**
	* Handles type selector parsing by creating a type selector node
	* and appending it to the current complex selector node.
	*
	* @param context Selector list parser context.
	*
	* @throws If the type selector is syntactically incorrect.
	*/
	static handle(context) {
		const { options, baseOffset, stream, complexSelector, isTypeSelectorSet } = context;
		const token = stream.getOrFail();
		if (isTypeSelectorSet) throw new AdblockSyntaxError("Type selector is already set for the compound selector", baseOffset + token.start, baseOffset + token.end);
		if (complexSelector.children.length !== 0 && complexSelector.children[complexSelector.children.length - 1].type !== "SelectorCombinator") throw new AdblockSyntaxError("Type selector must be first in the compound selector", baseOffset + token.start, baseOffset + token.end);
		const value = stream.fragment();
		const result = {
			type: "TypeSelector",
			value
		};
		if (options.isLocIncluded) {
			result.start = baseOffset + token.start;
			result.end = baseOffset + token.start + value.length;
		}
		complexSelector.children.push(result);
		stream.advance();
		context.isTypeSelectorSet = true;
	}
};
//#endregion
export { TypeSelectorHandler };
