import { StringUtils } from "../../utils/string.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { ModifierParser } from "./modifier-parser.js";
//#region node_modules/@adguard/agtree/dist/parser/misc/modifier-list.js
/**
* `ModifierListParser` is responsible for parsing modifier lists. Please note that the name is not
* uniform, "modifiers" are also known as "options".
*
* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#basic-rules-modifiers}
* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#non-basic-rules-modifiers}
* @see {@link https://help.eyeo.com/adblockplus/how-to-write-filters#options}
*/
var ModifierListParser = class extends BaseParser {
	/**
	* Parses the cosmetic rule modifiers, eg. `third-party,domain=example.com|~example.org`.
	*
	* _Note:_ you should remove `$` separator before passing the raw modifiers to this function,
	*  or it will be parsed in the first modifier.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @returns Parsed modifiers interface
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		const result = {
			type: "ModifierList",
			children: []
		};
		if (options.isLocIncluded) {
			result.start = baseOffset;
			result.end = baseOffset + raw.length;
		}
		let offset = StringUtils.skipWS(raw);
		let separatorIndex = -1;
		while (offset < raw.length) {
			offset = StringUtils.skipWS(raw, offset);
			const modifierStart = offset;
			let useSimpleSearch = false;
			const equalsIndex = raw.indexOf("=", offset);
			if (equalsIndex !== -1 && equalsIndex < raw.length - 1) {
				const valueStart = equalsIndex + 1;
				if (raw[valueStart] === "/" && raw[valueStart + 1] !== "/") {
					let firstClosingSlashIndex = -1;
					for (let i = valueStart + 1; i < raw.length; i += 1) if (raw[i] === "/" && raw[i - 1] !== "\\") {
						firstClosingSlashIndex = i;
						break;
					}
					if (firstClosingSlashIndex === -1) useSimpleSearch = true;
					else {
						let hasMoreSlashes = false;
						for (let i = firstClosingSlashIndex + 1; i < raw.length; i += 1) if (raw[i] === "/" && raw[i - 1] !== "\\") {
							hasMoreSlashes = true;
							break;
						}
						if (hasMoreSlashes) useSimpleSearch = true;
					}
				} else useSimpleSearch = true;
			}
			let nextSeparator;
			if (useSimpleSearch) nextSeparator = StringUtils.findNextUnescapedCharacter(raw, ",", offset);
			else nextSeparator = StringUtils.findUnescapedNonStringNonRegexChar(raw, ",", offset);
			separatorIndex = nextSeparator;
			const modifierEnd = separatorIndex === -1 ? raw.length : StringUtils.skipWSBack(raw, separatorIndex - 1) + 1;
			const modifier = ModifierParser.parse(raw.slice(modifierStart, modifierEnd), options, baseOffset + modifierStart);
			result.children.push(modifier);
			offset = separatorIndex === -1 ? raw.length : separatorIndex + 1;
		}
		if (separatorIndex !== -1) {
			const modifierStart = StringUtils.skipWS(raw, separatorIndex + 1);
			result.children.push(ModifierParser.parse(raw.slice(modifierStart, raw.length), options, baseOffset + modifierStart));
		}
		return result;
	}
};
//#endregion
export { ModifierListParser };
