import { TokenType } from "../../../../../../css-tokenizer/dist/csstokenizer.js";
//#region node_modules/@adguard/agtree/dist/parser/cosmetic/selector/handlers/class-selector-handler.js
/**
* Handles class selector parsing in selector list.
*/
var ClassSelectorHandler = class {
	/**
	* Handles class selector parsing by creating a class selector node
	* and appending it to the current complex selector node.
	*
	* @param context Selector list parser context.
	*
	* @throws If the class selector is syntactically incorrect.
	*/
	static handle(context) {
		const { options, baseOffset, stream, complexSelector } = context;
		const token = stream.getOrFail();
		stream.advance();
		stream.expect(TokenType.Ident);
		const value = stream.fragment();
		const result = {
			type: "ClassSelector",
			value
		};
		if (options.isLocIncluded) {
			result.start = baseOffset + token.start;
			result.end = baseOffset + token.end + value.length;
		}
		complexSelector.children.push(result);
		stream.advance();
	}
};
//#endregion
export { ClassSelectorHandler };
