import { AdblockSyntaxError } from "../../errors/adblock-syntax-error.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { require_sprintf } from "../../../../../sprintf-js/src/sprintf.js";
import { TokenType, getFormattedTokenName } from "../../../../css-tokenizer/dist/csstokenizer.js";
import { tokenizeFnBalanced } from "./balancing.js";
import { UboPseudoName } from "../../common/ubo-selector-common.js";
//#region node_modules/@adguard/agtree/dist/parser/css/ubo-selector-parser.js
var import_sprintf = require_sprintf();
/**
* @file Parser for special uBO selectors.
*/
/**
* Possible error messages for uBO selectors. Formatted with {@link sprintf}.
*/
var ERROR_MESSAGES = {
	DUPLICATED_UBO_MODIFIER: "uBO modifier '%s' cannot be used more than once",
	EXPECTED_BUT_GOT_BEFORE: "Expected '%s' but got '%s' before '%s'",
	NEGATED_UBO_MODIFIER_CANNOT_BE_FOLLOWED_BY: "Negated uBO modifier '%s' cannot be followed by anything else than a closing parenthesis or a whitespace",
	NEGATED_UBO_MODIFIER_CANNOT_BE_PRECEDED_BY: "Negated uBO modifier '%s' cannot be preceded by '%s'",
	PSEUDO_CANNOT_BE_NESTED: "uBO modifier '%s' cannot be nested inside '%s', only '%s' is allowed as a wrapper",
	UBO_MODIFIER_CANNOT_BE_NESTED: "uBO modifier '%s' cannot be nested",
	UBO_STYLE_CANNOT_BE_FOLLOWED: "uBO style injection cannot be followed by anything else than a whitespace"
};
/**
* Dummy parameter for uBO modifiers in error messages.
*/
var DUMMY_PARAM = "...";
/**
* Set of known uBO modifiers.
*
* @note We use `string` instead of `UboPseudoName` because we use this set for checking if a modifier is a known uBO,
* and an unknown sequence is just a string.
*/
var KNOWN_UBO_MODIFIERS = new Set([
	UboPseudoName.MatchesMedia,
	UboPseudoName.MatchesPath,
	UboPseudoName.Remove,
	UboPseudoName.Style
]);
/**
* Helper function to check if the given selector has any uBO modifier. This function should be fast, because it's used
* in the hot path of the parser.
*
* @param raw Raw selector string.
* @returns `true` if the selector has any uBO modifier, `false` otherwise.
*/
var hasAnyUboModifier = (raw) => {
	let colonIndex = raw.indexOf(":");
	while (colonIndex !== -1) {
		const openingParenthesisIndex = raw.indexOf("(", colonIndex + 1);
		if (openingParenthesisIndex === -1) return false;
		if (KNOWN_UBO_MODIFIERS.has(raw.slice(colonIndex + 1, openingParenthesisIndex))) return true;
		colonIndex = raw.indexOf(":", colonIndex + 1);
	}
	return false;
};
/**
* A simple helper function to format a pseudo name for error messages.
*
* @param name Pseudo name.
* @param wrapper Wrapper pseudo name (eg. `not`) (optional, defaults to `undefined`).
* @returns Formatted pseudo name.
* @example
* ```ts
* formatPseudoName('matches-path', 'not'); // => ':not(:matches-path(...))'
* formatPseudoName('matches-media'); // => ':matches-media(...)'
* ```
*/
var formatPseudoName = (name, wrapper) => {
	const result = [];
	if (wrapper) result.push(":", wrapper, "(");
	result.push(":", name, "(", DUMMY_PARAM, ")");
	if (wrapper) result.push(")");
	return result.join("");
};
/**
* Parser for uBO selectors.
*/
var UboSelectorParser = class extends BaseParser {
	/**
	* Parses a uBO selector list, eg. `div:matches-path(/path)`.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	*
	* @returns Parsed uBO selector {@link UboSelectorParser}.
	* @throws An {@link AdblockSyntaxError} if the selector list is syntactically invalid.
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		const modifiers = {
			type: "ModifierList",
			children: []
		};
		if (options.isLocIncluded) {
			modifiers.start = baseOffset;
			modifiers.end = baseOffset + raw.length;
		}
		if (!hasAnyUboModifier(raw)) {
			const selector = {
				type: "Value",
				value: raw
			};
			if (options.isLocIncluded) {
				selector.start = baseOffset;
				selector.end = baseOffset + raw.length;
			}
			const result = {
				type: "UboSelector",
				selector,
				modifiers
			};
			if (options.isLocIncluded) {
				result.start = baseOffset;
				result.end = baseOffset + raw.length;
			}
			return result;
		}
		const processedModifiers = /* @__PURE__ */ new Set();
		const tokens = [];
		const uboIndexes = new Array(raw.length);
		const uboModifierStack = [];
		let i = 0;
		const stackModifier = (modifier) => {
			if (processedModifiers.has(modifier.name)) throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.DUPLICATED_UBO_MODIFIER, formatPseudoName(modifier.name)), baseOffset + modifier.modifierStart, baseOffset + raw.length);
			uboModifierStack.push(modifier);
		};
		tokenizeFnBalanced(raw, (type, start, end, _, balance) => {
			if ((processedModifiers.has(UboPseudoName.Style) || processedModifiers.has(UboPseudoName.Remove)) && type !== TokenType.Whitespace) throw new AdblockSyntaxError(ERROR_MESSAGES.UBO_STYLE_CANNOT_BE_FOLLOWED, baseOffset + start, baseOffset + raw.length);
			if (tokens[i - 1]?.type === TokenType.Colon && type === TokenType.Function) {
				const fn = raw.slice(start, end - 1);
				if (KNOWN_UBO_MODIFIERS.has(fn)) if (balance > 1) if (fn === UboPseudoName.MatchesPath) {
					if (uboModifierStack.length > 0) throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.PSEUDO_CANNOT_BE_NESTED, formatPseudoName(UboPseudoName.MatchesPath), formatPseudoName(uboModifierStack[uboModifierStack.length - 1].name), formatPseudoName("not")), baseOffset + start - 1, baseOffset + raw.length);
					let isException = false;
					let modifierBalance = balance;
					let modifierStart = start;
					for (let j = i - 1; j >= 0; j -= 1) if (tokens[j].balance === 0) {
						modifierStart = tokens[j].start;
						modifierBalance = tokens[j].balance;
						break;
					} else if (tokens[j].type === TokenType.Colon || tokens[j].type === TokenType.Whitespace) continue;
					else if (tokens[j].type === TokenType.Function) {
						const wrapperFnName = raw.slice(tokens[j].start, tokens[j].end - 1);
						if (wrapperFnName !== "not") throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.PSEUDO_CANNOT_BE_NESTED, formatPseudoName(UboPseudoName.MatchesPath), formatPseudoName(wrapperFnName), formatPseudoName("not")), baseOffset + tokens[j].start - 1, baseOffset + raw.length);
						if (tokens[j - 1]?.type !== TokenType.Colon) {
							const got = tokens[j - 1]?.type ? getFormattedTokenName(tokens[j - 1]?.type) : "nothing";
							throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.EXPECTED_BUT_GOT_BEFORE, getFormattedTokenName(TokenType.Colon), got, formatPseudoName(UboPseudoName.MatchesPath, "not")), baseOffset + tokens[j - 1]?.start || 0, baseOffset + raw.length);
						}
						isException = !isException;
						continue;
					} else throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.NEGATED_UBO_MODIFIER_CANNOT_BE_PRECEDED_BY, formatPseudoName(UboPseudoName.MatchesPath), getFormattedTokenName(tokens[j].type)), baseOffset + tokens[j].start, baseOffset + raw.length);
					stackModifier({
						name: fn,
						modifierStart,
						modifierBalance,
						nameStart: start,
						nameEnd: end - 1,
						valueStart: end,
						valueBalance: balance,
						isException
					});
				} else throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.UBO_MODIFIER_CANNOT_BE_NESTED, formatPseudoName(fn)), baseOffset + start - 1, baseOffset + raw.length);
				else stackModifier({
					name: fn,
					modifierStart: start - 1,
					modifierBalance: balance,
					nameStart: start,
					nameEnd: end - 1,
					valueStart: end,
					valueBalance: balance,
					isException: false
				});
			} else {
				const lastStackedModifier = uboModifierStack[uboModifierStack.length - 1];
				if (lastStackedModifier?.name === UboPseudoName.MatchesPath && lastStackedModifier?.isException) {
					if (!(type === TokenType.CloseParenthesis || type === TokenType.Whitespace) && balance < lastStackedModifier.valueBalance) throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.NEGATED_UBO_MODIFIER_CANNOT_BE_FOLLOWED_BY, formatPseudoName(UboPseudoName.MatchesPath), getFormattedTokenName(type)), baseOffset + start, baseOffset + raw.length);
				}
				if (type === TokenType.CloseParenthesis && lastStackedModifier) {
					if (balance === Math.max(0, lastStackedModifier.valueBalance - 1)) lastStackedModifier.valueEnd = start;
					if (balance === Math.max(0, lastStackedModifier.modifierBalance - 1)) {
						const modifierName = {
							type: "Value",
							value: lastStackedModifier.name
						};
						if (options.isLocIncluded) {
							modifierName.start = baseOffset + lastStackedModifier.nameStart;
							modifierName.end = baseOffset + lastStackedModifier.nameEnd;
						}
						const value = {
							type: "Value",
							value: raw.slice(lastStackedModifier.valueStart, lastStackedModifier.valueEnd)
						};
						if (options.isLocIncluded) {
							value.start = baseOffset + lastStackedModifier.valueStart;
							value.end = baseOffset + lastStackedModifier.valueEnd;
						}
						const modifier = {
							type: "Modifier",
							name: modifierName,
							value,
							exception: lastStackedModifier.isException
						};
						if (options.isLocIncluded) {
							modifier.start = baseOffset + lastStackedModifier.modifierStart;
							modifier.end = baseOffset + end;
						}
						modifiers.children.push(modifier);
						processedModifiers.add(lastStackedModifier.name);
						uboModifierStack.pop();
						uboIndexes.fill(true, lastStackedModifier.modifierStart, end);
					}
				}
			}
			tokens.push({
				type,
				start,
				end,
				balance
			});
			i += 1;
		});
		const selector = {
			type: "Value",
			value: raw.split("").map((char, p) => uboIndexes[p] ? "" : char).join("").trim()
		};
		if (options.isLocIncluded) {
			selector.start = baseOffset;
			selector.end = baseOffset + raw.length;
		}
		const result = {
			type: "UboSelector",
			selector,
			modifiers
		};
		if (options.isLocIncluded) {
			result.start = baseOffset;
			result.end = baseOffset + raw.length;
		}
		return result;
	}
};
//#endregion
export { UboSelectorParser };
