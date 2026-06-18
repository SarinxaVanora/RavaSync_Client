import "../../../../../../virtual/_rolldown/runtime.js";
import { AdblockSyntax } from "../../utils/adblockers.js";
import "../../utils/constants.js";
import { CosmeticRuleSeparator } from "../../nodes/index.js";
import { isNull, isUndefined } from "../../utils/type-guards.js";
import { require_sprintf } from "../../../../../sprintf-js/src/sprintf.js";
import "../../../../css-tokenizer/dist/csstokenizer.js";
import { DomainListParser } from "../../parser/misc/domain-list-parser.js";
import { QuoteType, QuoteUtils } from "../../utils/quotes.js";
import "../../../../../tldts/dist/es6/index.js";
import { require_is_ip } from "../../../../../is-ip/index.js";
import { RuleConversionError } from "../../errors/rule-conversion-error.js";
import { GenericPlatform } from "../../compatibility-tables/platforms.js";
import { scriptletsCompatibilityTable } from "../../compatibility-tables/scriptlets.js";
import { RuleConverterBase } from "../base-interfaces/rule-converter-base.js";
import { createNodeConversionResult } from "../base-interfaces/conversion-result.js";
import { cloneDomainListNode, cloneModifierListNode, cloneScriptletRuleNode } from "../../ast-utils/clone.js";
import { getScriptletName, setScriptletName, setScriptletQuoteType, transformAllScriptletArguments, transformNthScriptletArgument } from "../../ast-utils/scriptlets.js";
require_sprintf();
require_is_ip();
/**
* @file Scriptlet injection rule converter
*/
var ABP_SCRIPTLET_PREFIX = "abp-";
var UBO_SCRIPTLET_PREFIX = "ubo-";
var UBO_SCRIPTLET_PREFIX_LENGTH = 4;
var UBO_SCRIPTLET_JS_SUFFIX = ".js";
var COMMA_SEPARATOR = ",";
var ADG_SET_CONSTANT_NAME = "set-constant";
var ADG_SET_CONSTANT_EMPTY_STRING = "";
var ADG_SET_CONSTANT_EMPTY_ARRAY = "emptyArr";
var ADG_SET_CONSTANT_EMPTY_OBJECT = "emptyObj";
var UBO_SET_CONSTANT_EMPTY_STRING = "''";
var UBO_SET_CONSTANT_EMPTY_ARRAY = "[]";
var UBO_SET_CONSTANT_EMPTY_OBJECT = "{}";
var ADG_PREVENT_FETCH_NAME = "prevent-fetch";
var ADG_PREVENT_FETCH_EMPTY_STRING = "";
var ADG_PREVENT_FETCH_WILDCARD = "*";
var UBO_NO_FETCH_IF_WILDCARD = "/^/";
var UBO_REMOVE_CLASS_NAME = "remove-class.js";
var UBO_REMOVE_ATTR_NAME = "remove-attr.js";
var setConstantAdgToUboMap = {
	[ADG_SET_CONSTANT_EMPTY_STRING]: UBO_SET_CONSTANT_EMPTY_STRING,
	[ADG_SET_CONSTANT_EMPTY_ARRAY]: UBO_SET_CONSTANT_EMPTY_ARRAY,
	[ADG_SET_CONSTANT_EMPTY_OBJECT]: UBO_SET_CONSTANT_EMPTY_OBJECT
};
var REMOVE_ATTR_CLASS_APPLYING = new Set([
	"asap",
	"stay",
	"complete"
]);
/**
* Scriptlet injection rule converter class
*
* @todo Implement `convertToUbo` and `convertToAbp`
*/
var ScriptletRuleConverter = class extends RuleConverterBase {
	/**
	* Converts a scriptlet injection rule to AdGuard format, if possible.
	*
	* @param rule Rule node to convert
	* @returns An object which follows the {@link NodeConversionResult} interface. Its `result` property contains
	* the array of converted rule nodes, and its `isConverted` flag indicates whether the original rule was converted.
	* If the rule was not converted, the result array will contain the original node with the same object reference
	* @throws If the rule is invalid or cannot be converted
	*/
	static convertToAdg(rule) {
		if (rule.syntax === AdblockSyntax.Adg) return createNodeConversionResult([rule], false);
		let convertedSeparator = rule.separator.value;
		convertedSeparator = rule.exception ? CosmeticRuleSeparator.AdgJsInjectionException : CosmeticRuleSeparator.AdgJsInjection;
		const convertedScriptlets = [];
		for (const scriptlet of rule.body.children) {
			const scriptletClone = cloneScriptletRuleNode(scriptlet);
			const scriptletName = QuoteUtils.setStringQuoteType(getScriptletName(scriptletClone), QuoteType.None);
			let prefix;
			let charToUnescape;
			switch (rule.syntax) {
				case AdblockSyntax.Abp:
					prefix = ABP_SCRIPTLET_PREFIX;
					charToUnescape = " ";
					break;
				case AdblockSyntax.Ubo:
					prefix = UBO_SCRIPTLET_PREFIX;
					charToUnescape = COMMA_SEPARATOR;
					break;
				default: prefix = "";
			}
			if (!scriptletName.startsWith(prefix)) setScriptletName(scriptletClone, `${prefix}${scriptletName}`);
			if (!isUndefined(charToUnescape)) transformAllScriptletArguments(scriptletClone, (value) => {
				if (!isNull(value)) return QuoteUtils.unescapeSingleEscapedOccurrences(value, charToUnescape);
				return value;
			});
			if (rule.syntax === AdblockSyntax.Ubo) {
				const scriptletData = scriptletsCompatibilityTable.getFirst(scriptletName, GenericPlatform.UboAny);
				if (scriptletData && (scriptletData.name === UBO_REMOVE_CLASS_NAME || scriptletData.name === UBO_REMOVE_ATTR_NAME) && scriptletClone.children.length > 2) {
					const selectors = [];
					let applying = null;
					let lastArg = scriptletClone.children.pop();
					if (lastArg) if (REMOVE_ATTR_CLASS_APPLYING.has(lastArg.value)) applying = lastArg.value;
					else selectors.push(lastArg.value);
					while (scriptletClone.children.length > 2) {
						lastArg = scriptletClone.children.pop();
						if (lastArg) selectors.push(lastArg.value.trim());
					}
					if (selectors.length > 0) scriptletClone.children.push({
						type: "Value",
						value: selectors.reverse().join(", ")
					});
					if (!isNull(applying)) {
						if (selectors.length === 0) scriptletClone.children.push({
							type: "Value",
							value: ""
						});
						scriptletClone.children.push({
							type: "Value",
							value: applying
						});
					}
				}
			}
			setScriptletQuoteType(scriptletClone, QuoteType.Single);
			convertedScriptlets.push(scriptletClone);
		}
		if (rule.body.children.length === 0) {
			const convertedScriptletNode = {
				category: rule.category,
				type: rule.type,
				syntax: AdblockSyntax.Adg,
				exception: rule.exception,
				domains: cloneDomainListNode(rule.domains),
				separator: {
					type: "Value",
					value: convertedSeparator
				},
				body: {
					type: rule.body.type,
					children: []
				}
			};
			if (rule.modifiers) convertedScriptletNode.modifiers = cloneModifierListNode(rule.modifiers);
			return createNodeConversionResult([convertedScriptletNode], true);
		}
		return createNodeConversionResult(convertedScriptlets.map((scriptlet) => {
			const res = {
				category: rule.category,
				type: rule.type,
				syntax: AdblockSyntax.Adg,
				exception: rule.exception,
				domains: cloneDomainListNode(rule.domains),
				separator: {
					type: "Value",
					value: convertedSeparator
				},
				body: {
					type: rule.body.type,
					children: [scriptlet]
				}
			};
			if (rule.modifiers) res.modifiers = cloneModifierListNode(rule.modifiers);
			return res;
		}), true);
	}
	/**
	* Converts a scriptlet injection rule to uBlock format, if possible.
	*
	* @param rule Rule node to convert
	* @returns An object which follows the {@link NodeConversionResult} interface. Its `result` property contains
	* the array of converted rule nodes, and its `isConverted` flag indicates whether the original rule was converted.
	* If the rule was not converted, the result array will contain the original node with the same object reference
	* @throws If the rule is invalid or cannot be converted
	*/
	static convertToUbo(rule) {
		if (rule.syntax === AdblockSyntax.Ubo) return createNodeConversionResult([rule], false);
		let ruleDomainsList = cloneDomainListNode(rule.domains);
		if (rule.syntax === AdblockSyntax.Adg && rule.modifiers?.children.length) {
			const { modifiers } = rule;
			if (!modifiers || !modifiers.children || modifiers.children.length === 0) throw new RuleConversionError("Invalid modifiers in AdGuard rule.");
			const [domainModifier] = modifiers.children;
			if (!(modifiers.children.length === 1 && domainModifier.name?.value === "domain" && domainModifier.value?.value)) throw new RuleConversionError("uBlock Origin scriptlet injection rules do not support cosmetic rule modifiers.");
			if (!domainModifier.value?.value) throw new RuleConversionError("Invalid domain modifier in AdGuard rule.");
			const parsedDomainList = DomainListParser.parse(domainModifier.value.value, {}, domainModifier.start, "|");
			if (ruleDomainsList) ruleDomainsList.children.push(...parsedDomainList.children);
			else ruleDomainsList = parsedDomainList;
		}
		let convertedSeparator = rule.separator.value;
		convertedSeparator = rule.exception ? CosmeticRuleSeparator.ElementHidingException : CosmeticRuleSeparator.ElementHiding;
		const convertedScriptlets = [];
		for (const scriptlet of rule.body.children) {
			const scriptletClone = cloneScriptletRuleNode(scriptlet);
			const scriptletName = QuoteUtils.setStringQuoteType(getScriptletName(scriptletClone), QuoteType.None);
			let uboScriptletName;
			if (rule.syntax === AdblockSyntax.Adg && scriptletName.startsWith(UBO_SCRIPTLET_PREFIX)) uboScriptletName = scriptletName.slice(UBO_SCRIPTLET_PREFIX_LENGTH);
			else {
				const uboScriptlet = scriptletsCompatibilityTable.getFirst(scriptletName, GenericPlatform.UboAny);
				if (!uboScriptlet) throw new RuleConversionError(`Scriptlet "${scriptletName}" is not supported in uBlock Origin.`);
				uboScriptletName = uboScriptlet.name;
			}
			if (uboScriptletName.endsWith(UBO_SCRIPTLET_JS_SUFFIX)) uboScriptletName = uboScriptletName.slice(0, -3);
			setScriptletName(scriptletClone, uboScriptletName);
			setScriptletQuoteType(scriptletClone, QuoteType.None);
			transformAllScriptletArguments(scriptletClone, (value) => {
				if (!isNull(value)) return QuoteUtils.escapeUnescapedOccurrences(value, COMMA_SEPARATOR);
				return value;
			});
			if (rule.syntax === AdblockSyntax.Abp) transformAllScriptletArguments(scriptletClone, (value) => {
				if (!isNull(value)) return QuoteUtils.unescapeSingleEscapedOccurrences(value, " ");
				return value;
			});
			switch (scriptletName) {
				case ADG_SET_CONSTANT_NAME:
					transformNthScriptletArgument(scriptletClone, 2, (value) => {
						if (!isNull(value)) return setConstantAdgToUboMap[value] ?? value;
						return value;
					});
					break;
				case ADG_PREVENT_FETCH_NAME:
					transformNthScriptletArgument(scriptletClone, 1, (value) => {
						if (value === ADG_PREVENT_FETCH_EMPTY_STRING || value === ADG_PREVENT_FETCH_WILDCARD) return UBO_NO_FETCH_IF_WILDCARD;
						return value;
					});
					break;
			}
			convertedScriptlets.push(scriptletClone);
		}
		if (rule.body.children.length === 0) {
			const convertedScriptletNode = {
				category: rule.category,
				type: rule.type,
				syntax: AdblockSyntax.Ubo,
				exception: rule.exception,
				domains: cloneDomainListNode(rule.domains),
				separator: {
					type: "Value",
					value: convertedSeparator
				},
				body: {
					type: rule.body.type,
					children: []
				}
			};
			if (rule.modifiers) convertedScriptletNode.modifiers = cloneModifierListNode(rule.modifiers);
			return createNodeConversionResult([convertedScriptletNode], true);
		}
		return createNodeConversionResult(convertedScriptlets.map((scriptlet) => {
			return {
				category: rule.category,
				type: rule.type,
				syntax: AdblockSyntax.Ubo,
				exception: rule.exception,
				domains: ruleDomainsList,
				separator: {
					type: "Value",
					value: convertedSeparator
				},
				body: {
					type: rule.body.type,
					children: [scriptlet]
				}
			};
		}), true);
	}
};
//#endregion
export { ScriptletRuleConverter };
