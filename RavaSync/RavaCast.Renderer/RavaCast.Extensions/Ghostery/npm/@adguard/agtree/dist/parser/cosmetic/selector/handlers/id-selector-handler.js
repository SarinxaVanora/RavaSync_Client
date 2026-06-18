//#region node_modules/@adguard/agtree/dist/parser/cosmetic/selector/handlers/id-selector-handler.js
/**
* Handles ID selector parsing in selector list.
*/
var IdSelectorHandler = class {
	/**
	* Handles ID selector parsing by creating an ID selector node
	* and appending it to the current complex selector node.
	*
	* @param context Selector list parser context.
	*
	* @throws If the ID selector is syntactically incorrect.
	*/
	static handle(context) {
		const { raw, options, baseOffset, stream, complexSelector } = context;
		const token = stream.getOrFail();
		const result = {
			type: "IdSelector",
			value: raw.slice(token.start + 1, token.end)
		};
		if (options.isLocIncluded) {
			result.start = baseOffset + token.start;
			result.end = baseOffset + token.end;
		}
		complexSelector.children.push(result);
		stream.advance();
	}
};
//#endregion
export { IdSelectorHandler };
