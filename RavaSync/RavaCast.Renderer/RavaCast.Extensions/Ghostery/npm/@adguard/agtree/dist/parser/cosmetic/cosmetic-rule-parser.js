import { AdblockSyntax } from "../../utils/adblockers.js";
import "../../utils/constants.js";
import { StringUtils } from "../../utils/string.js";
import { CosmeticRuleType, RuleCategory } from "../../nodes/index.js";
import { AdblockSyntaxError } from "../../errors/adblock-syntax-error.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
import { CosmeticRuleSeparatorUtils } from "../../utils/cosmetic-rule-separator.js";
import { CommentParser } from "../comment/comment-parser.js";
import { require_sprintf } from "../../../../../sprintf-js/src/sprintf.js";
import { TokenType, hasToken } from "../../../../css-tokenizer/dist/csstokenizer.js";
import { DomainListParser } from "../misc/domain-list-parser.js";
import { ModifierListParser } from "../misc/modifier-list.js";
import { UboPseudoName } from "../../common/ubo-selector-common.js";
import { UboSelectorParser } from "../css/ubo-selector-parser.js";
import { AdgCssInjectionParser } from "../css/adg-css-injection-parser.js";
import { AbpSnippetInjectionBodyParser } from "./scriptlet-body/abp-snippet-injection-body-parser.js";
import { UboScriptletInjectionBodyParser } from "./scriptlet-body/ubo-scriptlet-injection-body-parser.js";
import { AdgScriptletInjectionBodyParser } from "./scriptlet-body/adg-scriptlet-injection-body-parser.js";
import { AdgHtmlFilteringBodyParser } from "./html-filtering-body/adg-html-filtering-body-parser.js";
import { UboHtmlFilteringBodyParser } from "./html-filtering-body/ubo-html-filtering-body-parser.js";
//#region node_modules/@adguard/agtree/dist/parser/cosmetic/cosmetic-rule-parser.js
var import_sprintf = require_sprintf();
/**
* Possible error messages for uBO selectors. Formatted with {@link sprintf}.
*/
var ERROR_MESSAGES = {
	EMPTY_RULE_BODY: "Empty rule body",
	INVALID_BODY_FOR_SEPARATOR: "Body '%s' is not valid for the '%s' cosmetic rule separator",
	MISSING_ADGUARD_MODIFIER_LIST_END: "Missing '%s' at the end of the AdGuard modifier list in pattern '%s'",
	MISSING_ADGUARD_MODIFIER_LIST_MARKER: "Missing '%s' at the beginning of the AdGuard modifier list in pattern '%s'",
	SYNTAXES_CANNOT_BE_MIXED: "'%s' syntax cannot be mixed with '%s' syntax",
	SYNTAX_DISABLED: "Parsing '%s' syntax is disabled, but the rule uses it"
};
var ADG_CSS_INJECTION_PATTERN = /^(?:.+){(?:.+)}$/;
/**
* `CosmeticRuleParser` is responsible for parsing cosmetic rules.
*
* Where possible, it automatically detects the difference between supported syntaxes:
*  - AdGuard
*  - uBlock Origin
*  - Adblock Plus
*
* If the syntax is common / cannot be determined, the parser gives `Common` syntax.
*
* Please note that syntactically correct rules are parsed even if they are not actually
* compatible with the given adblocker. This is a completely natural behavior, meaningful
* checking of compatibility is not done at the parser level.
*/
var CosmeticRuleParser = class extends BaseParser {
	/**
	* Determines whether a rule is a cosmetic rule. The rule is considered cosmetic if it
	* contains a cosmetic rule separator.
	*
	* @param raw Raw rule
	* @returns `true` if the rule is a cosmetic rule, `false` otherwise
	*/
	static isCosmeticRule(raw) {
		const trimmed = raw.trim();
		if (CommentParser.isCommentRule(trimmed)) return false;
		return CosmeticRuleSeparatorUtils.find(trimmed) !== null;
	}
	/**
	* Parses a cosmetic rule. The structure of the cosmetic rules:
	*  - pattern (AdGuard pattern can have modifiers, other syntaxes don't)
	*  - separator
	*  - body
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @returns
	* Parsed cosmetic rule AST or null if it failed to parse based on the known cosmetic rules
	* @throws If the input matches the cosmetic rule pattern but syntactically invalid
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		const separatorResult = CosmeticRuleSeparatorUtils.find(raw);
		if (!separatorResult) return null;
		let syntax = AdblockSyntax.Common;
		let modifiers;
		const patternStart = StringUtils.skipWS(raw);
		const patternEnd = StringUtils.skipWSBack(raw, separatorResult.start - 1) + 1;
		const bodyStart = StringUtils.skipWS(raw, separatorResult.end);
		const bodyEnd = StringUtils.skipWSBack(raw) + 1;
		if (bodyEnd <= bodyStart) throw new AdblockSyntaxError(ERROR_MESSAGES.EMPTY_RULE_BODY, baseOffset, baseOffset + raw.length);
		const rawPattern = raw.slice(patternStart, patternEnd);
		let patternOffset = patternStart;
		if (rawPattern[patternOffset] === "[") {
			const modifierListStart = patternOffset;
			patternOffset += 1;
			patternOffset = StringUtils.skipWS(rawPattern, patternOffset);
			if (rawPattern[patternOffset] !== "$") throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.MISSING_ADGUARD_MODIFIER_LIST_MARKER, "$", rawPattern), baseOffset + patternOffset, baseOffset + rawPattern.length);
			patternOffset += 1;
			patternOffset = StringUtils.skipWS(rawPattern, patternOffset);
			const modifierListEnd = StringUtils.findLastUnescapedNonStringNonRegexChar(rawPattern, "]");
			if (modifierListEnd === -1) throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.MISSING_ADGUARD_MODIFIER_LIST_END, "]", rawPattern), baseOffset + patternOffset, baseOffset + rawPattern.length);
			modifiers = ModifierListParser.parse(raw.slice(patternOffset, modifierListEnd), options, baseOffset + patternOffset);
			if (options.isLocIncluded) {
				modifiers.start = baseOffset + modifierListStart;
				modifiers.end = baseOffset + modifierListEnd + 1;
			}
			patternOffset = modifierListEnd + 1;
			syntax = AdblockSyntax.Adg;
		}
		patternOffset = StringUtils.skipWS(rawPattern, patternOffset);
		const domains = DomainListParser.parse(rawPattern.slice(patternOffset), options, baseOffset + patternOffset);
		const separator = {
			type: "Value",
			value: separatorResult.separator
		};
		if (options.isLocIncluded) {
			separator.start = baseOffset + separatorResult.start;
			separator.end = baseOffset + separatorResult.end;
		}
		const exception = CosmeticRuleSeparatorUtils.isException(separatorResult.separator);
		let rawBody = raw.slice(bodyStart, bodyEnd);
		/**
		* Ensures that the rule syntax is common or the expected one. This function is used to prevent mixing
		* different syntaxes in the same rule.
		*
		* @example
		* The following rule mixes AdGuard and uBO syntaxes, because it uses AdGuard modifier list and uBO
		* CSS injection:
		* ```adblock
		* [$path=/something]example.com##.foo:style(color: red)
		* ```
		* In this case, parser sets syntax to AdGuard, because it detects the AdGuard modifier list, but
		* when parsing the rule body, it detects uBO CSS injection, which is not compatible with AdGuard.
		*
		* @param expectedSyntax Expected syntax
		* @throws If the rule syntax is not common or the expected one
		*/
		const expectCommonOrSpecificSyntax = (expectedSyntax) => {
			if (syntax !== AdblockSyntax.Common && syntax !== expectedSyntax) throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.SYNTAXES_CANNOT_BE_MIXED, expectedSyntax, syntax), baseOffset + patternStart, baseOffset + bodyEnd);
		};
		let uboSelector;
		if (options.parseUboSpecificRules) {
			uboSelector = UboSelectorParser.parse(rawBody, options, baseOffset + bodyStart);
			rawBody = uboSelector.selector.value;
			if (uboSelector.modifiers && uboSelector.modifiers.children.length > 0) {
				expectCommonOrSpecificSyntax(AdblockSyntax.Ubo);
				syntax = AdblockSyntax.Ubo;
				for (const modifier of uboSelector.modifiers.children) if (modifier.name.value === UboPseudoName.MatchesPath) {
					if (!modifiers) {
						modifiers = {
							type: "ModifierList",
							children: []
						};
						if (options.isLocIncluded) {
							modifiers.start = baseOffset + bodyStart;
							modifiers.end = baseOffset + bodyEnd;
						}
					}
					modifiers.children.push(modifier);
				}
			}
		}
		const raws = { text: raw };
		const baseRule = {
			category: RuleCategory.Cosmetic,
			exception,
			modifiers,
			domains,
			separator
		};
		if (options.includeRaws) baseRule.raws = raws;
		if (options.isLocIncluded) {
			baseRule.start = baseOffset;
			baseRule.end = baseOffset + raw.length;
		}
		const parseUboCssInjection = () => {
			if (!uboSelector || !uboSelector.modifiers || uboSelector.modifiers.children?.length < 1) return null;
			expectCommonOrSpecificSyntax(AdblockSyntax.Ubo);
			const selectorList = uboSelector.selector;
			let declarationList;
			let mediaQueryList;
			let remove = false;
			for (const modifier of uboSelector.modifiers.children) switch (modifier.name.value) {
				case UboPseudoName.Style:
					declarationList = modifier.value;
					break;
				case UboPseudoName.Remove:
					declarationList = {
						type: "Value",
						value: ""
					};
					remove = true;
					break;
				case UboPseudoName.MatchesMedia:
					mediaQueryList = modifier.value;
					break;
			}
			if (!declarationList) return null;
			const body = {
				type: "CssInjectionRuleBody",
				selectorList,
				declarationList,
				mediaQueryList,
				remove
			};
			if (options.isLocIncluded) {
				body.start = baseOffset + bodyStart;
				body.end = baseOffset + bodyEnd;
			}
			return {
				syntax: AdblockSyntax.Ubo,
				type: CosmeticRuleType.CssInjectionRule,
				body
			};
		};
		const parseElementHiding = () => {
			const selectorList = {
				type: "Value",
				value: rawBody
			};
			if (options.isLocIncluded) {
				selectorList.start = baseOffset + bodyStart;
				selectorList.end = baseOffset + bodyEnd;
			}
			const body = {
				type: "ElementHidingRuleBody",
				selectorList
			};
			if (options.isLocIncluded) {
				body.start = baseOffset + bodyStart;
				body.end = baseOffset + bodyEnd;
			}
			return {
				syntax,
				type: CosmeticRuleType.ElementHidingRule,
				body
			};
		};
		const parseAdgCssInjection = () => {
			if (!ADG_CSS_INJECTION_PATTERN.test(rawBody)) return null;
			expectCommonOrSpecificSyntax(AdblockSyntax.Adg);
			return {
				syntax: AdblockSyntax.Adg,
				type: CosmeticRuleType.CssInjectionRule,
				body: AdgCssInjectionParser.parse(rawBody, options, baseOffset + bodyStart)
			};
		};
		/**
		* Parses Adb CSS injection rules
		* eg: example.com##.foo { display: none; }
		*
		* @returns parsed rule
		*/
		const parseAbpCssInjection = () => {
			if (!options.parseAbpSpecificRules) return null;
			if (rawBody.indexOf("{") === -1 && rawBody.indexOf("}") === -1) return null;
			if (!hasToken(rawBody, new Set([TokenType.OpenCurlyBracket, TokenType.CloseCurlyBracket]))) return null;
			const body = AdgCssInjectionParser.parse(rawBody, options, baseOffset + bodyStart);
			return {
				syntax: AdblockSyntax.Abp,
				type: CosmeticRuleType.CssInjectionRule,
				body
			};
		};
		const parseAbpSnippetInjection = () => {
			if (!options.parseAbpSpecificRules) throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.SYNTAX_DISABLED, AdblockSyntax.Abp), baseOffset + bodyStart, baseOffset + bodyEnd);
			expectCommonOrSpecificSyntax(AdblockSyntax.Abp);
			const body = AbpSnippetInjectionBodyParser.parse(rawBody, options, baseOffset + bodyStart);
			if (options.isLocIncluded) {
				body.start = baseOffset + bodyStart;
				body.end = baseOffset + bodyEnd;
			}
			return {
				syntax: AdblockSyntax.Abp,
				type: CosmeticRuleType.ScriptletInjectionRule,
				body
			};
		};
		const parseUboScriptletInjection = () => {
			if (!rawBody.startsWith("+js") && !rawBody.startsWith("script:inject")) return null;
			if (!options.parseUboSpecificRules) throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.SYNTAX_DISABLED, AdblockSyntax.Ubo), baseOffset + bodyStart, baseOffset + bodyEnd);
			expectCommonOrSpecificSyntax(AdblockSyntax.Ubo);
			const body = UboScriptletInjectionBodyParser.parse(rawBody, options, baseOffset + bodyStart);
			if (options.isLocIncluded) {
				body.start = baseOffset + bodyStart;
				body.end = baseOffset + bodyEnd;
			}
			return {
				syntax: AdblockSyntax.Ubo,
				type: CosmeticRuleType.ScriptletInjectionRule,
				body
			};
		};
		const parseAdgScriptletInjection = () => {
			if (!rawBody.startsWith("//scriptlet")) return null;
			expectCommonOrSpecificSyntax(AdblockSyntax.Adg);
			const body = AdgScriptletInjectionBodyParser.parse(rawBody, options, baseOffset + bodyStart);
			if (options.isLocIncluded) {
				body.start = baseOffset + bodyStart;
				body.end = baseOffset + bodyEnd;
			}
			return {
				syntax: AdblockSyntax.Adg,
				type: CosmeticRuleType.ScriptletInjectionRule,
				body
			};
		};
		const parseAdgJsInjection = () => {
			expectCommonOrSpecificSyntax(AdblockSyntax.Adg);
			const body = {
				type: "Value",
				value: rawBody
			};
			if (options.isLocIncluded) {
				body.start = baseOffset + bodyStart;
				body.end = baseOffset + bodyEnd;
			}
			return {
				syntax: AdblockSyntax.Adg,
				type: CosmeticRuleType.JsInjectionRule,
				body
			};
		};
		const parseUboHtmlFiltering = () => {
			if (!rawBody.startsWith("^")) return null;
			if (!options.parseUboSpecificRules) throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.SYNTAX_DISABLED, AdblockSyntax.Ubo), baseOffset + bodyStart, baseOffset + bodyEnd);
			expectCommonOrSpecificSyntax(AdblockSyntax.Ubo);
			const rawBodyWithoutMask = rawBody.slice(1);
			const body = UboHtmlFilteringBodyParser.parse(rawBodyWithoutMask, options, baseOffset + bodyStart + 1);
			return {
				syntax: AdblockSyntax.Ubo,
				type: CosmeticRuleType.HtmlFilteringRule,
				body
			};
		};
		const parseAdgHtmlFiltering = () => {
			expectCommonOrSpecificSyntax(AdblockSyntax.Adg);
			const body = AdgHtmlFilteringBodyParser.parse(rawBody, options, baseOffset + bodyStart);
			return {
				syntax: AdblockSyntax.Adg,
				type: CosmeticRuleType.HtmlFilteringRule,
				body
			};
		};
		const parseFunctions = {
			"##": [
				parseUboHtmlFiltering,
				parseUboScriptletInjection,
				parseUboCssInjection,
				parseAbpCssInjection,
				parseElementHiding
			],
			"#@#": [
				parseUboHtmlFiltering,
				parseUboScriptletInjection,
				parseUboCssInjection,
				parseAbpCssInjection,
				parseElementHiding
			],
			"#?#": [
				parseUboCssInjection,
				parseAbpCssInjection,
				parseElementHiding
			],
			"#@?#": [
				parseUboCssInjection,
				parseAbpCssInjection,
				parseElementHiding
			],
			"#$#": [parseAdgCssInjection, parseAbpSnippetInjection],
			"#@$#": [parseAdgCssInjection, parseAbpSnippetInjection],
			"#$?#": [parseAdgCssInjection],
			"#@$?#": [parseAdgCssInjection],
			"#%#": [parseAdgScriptletInjection, parseAdgJsInjection],
			"#@%#": [parseAdgScriptletInjection, parseAdgJsInjection],
			$$: [parseAdgHtmlFiltering],
			"$@$": [parseAdgHtmlFiltering]
		}[separatorResult.separator];
		let restProps;
		for (const parseFunction of parseFunctions) {
			restProps = parseFunction();
			if (restProps) break;
		}
		if (!restProps) throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.INVALID_BODY_FOR_SEPARATOR, rawBody, separatorResult.separator), baseOffset + bodyStart, baseOffset + bodyEnd);
		return {
			...baseRule,
			...restProps
		};
	}
};
//#endregion
export { CosmeticRuleParser };
