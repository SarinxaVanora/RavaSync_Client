import { AdblockSyntax } from "../../utils/adblockers.js";
import { StringUtils } from "../../utils/string.js";
import { NetworkRuleType, RuleCategory } from "../../nodes/index.js";
import { AdblockSyntaxError } from "../../errors/adblock-syntax-error.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { ValueParser } from "../misc/value-parser.js";
import { ModifierListParser } from "../misc/modifier-list.js";
//#region node_modules/@adguard/agtree/dist/parser/network/network-rule-parser.js
/**
* `NetworkRuleParser` is responsible for parsing network rules.
*
* Please note that this will parse all syntactically correct network rules.
* Modifier compatibility is not checked at the parser level.
*
* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#basic-rules}
* @see {@link https://help.eyeo.com/adblockplus/how-to-write-filters#basic}
*/
var NetworkRuleParser = class NetworkRuleParser extends BaseParser {
	/**
	* Parses a network rule (also known as basic rule).
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @returns Network rule AST
	*
	* @throws If the rule is syntactically incorrect.
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		let offset = 0;
		offset = StringUtils.skipWS(raw, offset);
		let exception = false;
		if (raw.startsWith("@@", offset)) {
			offset += 2;
			exception = true;
		}
		const patternStart = offset;
		const separatorIndex = NetworkRuleParser.findNetworkRuleSeparatorIndex(raw);
		const patternEnd = separatorIndex === -1 ? StringUtils.skipWSBack(raw) + 1 : StringUtils.skipWSBack(raw, separatorIndex - 1) + 1;
		const pattern = ValueParser.parse(raw.slice(patternStart, patternEnd), options, baseOffset + patternStart);
		let modifiers;
		const lastNonWsIndex = StringUtils.skipWSBack(raw);
		const modifiersStart = separatorIndex + 1;
		const modifiersEnd = StringUtils.skipWSBack(raw) + 1;
		if (separatorIndex !== -1) {
			if (separatorIndex === lastNonWsIndex) throw new AdblockSyntaxError("Empty modifiers are not allowed", baseOffset + separatorIndex, baseOffset + raw.length);
			modifiers = ModifierListParser.parse(raw.slice(modifiersStart, modifiersEnd), options, baseOffset + modifiersStart);
		}
		if (pattern.value.length === 0 && (modifiers === void 0 || modifiers.children.length === 0)) throw new AdblockSyntaxError("Network rule must have a pattern or modifiers", baseOffset, baseOffset + raw.length);
		const result = {
			type: NetworkRuleType.NetworkRule,
			category: RuleCategory.Network,
			syntax: AdblockSyntax.Common,
			exception,
			pattern,
			modifiers
		};
		if (options.includeRaws) result.raws = { text: raw };
		if (options.isLocIncluded) {
			result.start = baseOffset;
			result.end = baseOffset + raw.length;
		}
		return result;
	}
	/**
	* Finds the index of the separator character in a network rule.
	*
	* @param rule Network rule to check
	* @returns The index of the separator character, or -1 if there is no separator
	*/
	static findNetworkRuleSeparatorIndex(rule) {
		for (let i = rule.length - 1; i >= 0; i -= 1) if (rule[i] === "$" && rule[i + 1] !== "/" && rule[i - 1] !== "\\") return i;
		return -1;
	}
};
//#endregion
export { NetworkRuleParser };
