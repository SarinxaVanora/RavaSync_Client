import { isUndefined } from "../../utils/type-guards.js";
import { RuleConversionError } from "../../errors/rule-conversion-error.js";
import { GenericPlatform } from "../../compatibility-tables/platforms.js";
import { modifiersCompatibilityTable } from "../../compatibility-tables/modifiers.js";
import { isValidResourceType } from "../../compatibility-tables/utils/resource-type-helpers.js";
import { redirectsCompatibilityTable } from "../../compatibility-tables/redirects.js";
import { BaseConverter } from "../base-interfaces/base-converter.js";
import { createConversionResult } from "../base-interfaces/conversion-result.js";
import { cloneModifierListNode } from "../../ast-utils/clone.js";
import { createModifierNode } from "../../ast-utils/modifiers.js";
import { MultiValueMap } from "../../utils/multi-value-map.js";
//#region node_modules/@adguard/agtree/dist/converter/misc/network-rule-modifier.js
/**
* @file Network rule modifier list converter.
*/
/**
* @see {@link https://adguard.com/kb/general/ad-filtering/create-own-filters/#csp-modifier}
*/
var CSP_MODIFIER = "csp";
var CSP_SEPARATOR = "; ";
/**
* @see {@link https://adguard.com/kb/general/ad-filtering/create-own-filters/#csp-modifier}
* @see {@link https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Security-Policy}
*/
var COMMON_CSP_PARAMS = "'self' 'unsafe-eval' http: https: data: blob: mediastream: filesystem:";
/**
* @see {@link https://help.adblockplus.org/hc/en-us/articles/360062733293#rewrite}
*/
var ABP_REWRITE_MODIFIER = "rewrite";
/**
* @see {@link https://adguard.com/kb/general/ad-filtering/create-own-filters/#redirect-modifier}
*/
var REDIRECT_MODIFIER = "redirect";
/**
* @see {@link https://adguard.com/kb/general/ad-filtering/create-own-filters/#redirect-rule-modifier}
*/
var REDIRECT_RULE_MODIFIER = "redirect-rule";
/**
* @see {@link https://github.com/gorhill/uBlock/wiki/Resources-Library#empty-redirect-resources}
*/
var UBO_NOOP_TEXT_RESOURCE = "noop.txt";
/**
* Redirect-related modifiers.
*/
var REDIRECT_MODIFIERS = new Set([
	ABP_REWRITE_MODIFIER,
	REDIRECT_MODIFIER,
	REDIRECT_RULE_MODIFIER
]);
/**
* Conversion map for ADG network rule modifiers.
*/
var ADG_CONVERSION_MAP = new Map([
	["1p", [{
		name: () => "third-party",
		exception: (actual) => !actual
	}]],
	["3p", [{ name: () => "third-party" }]],
	["css", [{ name: () => "stylesheet" }]],
	["doc", [{ name: () => "document" }]],
	["ehide", [{ name: () => "elemhide" }]],
	["empty", [{
		name: () => "redirect",
		value: () => "nooptext"
	}]],
	["first-party", [{
		name: () => "third-party",
		exception: (actual) => !actual
	}]],
	["frame", [{ name: () => "subdocument" }]],
	["ghide", [{ name: () => "generichide" }]],
	["inline-font", [{
		name: () => CSP_MODIFIER,
		value: () => `font-src ${COMMON_CSP_PARAMS}`
	}]],
	["inline-script", [{
		name: () => CSP_MODIFIER,
		value: () => `script-src ${COMMON_CSP_PARAMS}`
	}]],
	["mp4", [{
		name: () => "redirect",
		value: () => "noopmp4-1s"
	}, {
		name: () => "media",
		value: () => void 0
	}]],
	["queryprune", [{ name: () => "removeparam" }]],
	["shide", [{ name: () => "specifichide" }]],
	["xhr", [{ name: () => "xmlhttprequest" }]]
]);
/**
* Helper class for converting network rule modifier lists.
*
* @todo Implement `convertToUbo` and `convertToAbp`
*/
var NetworkRuleModifierListConverter = class extends BaseConverter {
	/**
	* Converts a network rule modifier list to AdGuard format, if possible.
	*
	* @param modifierList Network rule modifier list node to convert
	* @param isException If `true`, the rule is an exception rule
	* @returns An object which follows the {@link ConversionResult} interface. Its `result` property contains
	* the converted node, and its `isConverted` flag indicates whether the original node was converted.
	* If the node was not converted, the result will contain the original node with the same object reference
	* @throws If the conversion is not possible
	*/
	static convertToAdg(modifierList, isException = false) {
		const conversionMap = new MultiValueMap();
		let cspCount = 0;
		modifierList.children.forEach((modifierNode, index) => {
			const modifierConversions = ADG_CONVERSION_MAP.get(modifierNode.name.value);
			if (modifierConversions) {
				for (const modifierConversion of modifierConversions) {
					const name = modifierConversion.name(modifierNode.name.value);
					const exception = modifierConversion.exception ? modifierConversion.exception(modifierNode.exception || false) : modifierNode.exception;
					const value = modifierConversion.value ? modifierConversion.value(modifierNode.value?.value) : modifierNode.value?.value;
					if (name !== modifierNode.name.value || value !== modifierNode.value?.value) conversionMap.add(index, createModifierNode(name, value, exception));
					if (name === CSP_MODIFIER) cspCount += 1;
				}
				return;
			}
			if (REDIRECT_MODIFIERS.has(modifierNode.name.value)) {
				if (modifierNode.exception === true) throw new RuleConversionError(`Modifier '${modifierNode.name.value}' cannot be negated`);
				const redirectResource = modifierNode.value?.value;
				if (!redirectResource && !isException) throw new RuleConversionError(`No redirect resource specified for '${modifierNode.name.value}' modifier`);
				const modifierName = modifierNode.name.value === ABP_REWRITE_MODIFIER ? REDIRECT_MODIFIER : modifierNode.name.value;
				const convertedRedirectResource = redirectResource ? redirectsCompatibilityTable.getFirst(redirectResource, GenericPlatform.AdgAny)?.name : void 0;
				if (modifierName !== modifierNode.name.value || convertedRedirectResource !== void 0 && convertedRedirectResource !== redirectResource) conversionMap.add(index, createModifierNode(modifierName, convertedRedirectResource || redirectResource, modifierNode.exception));
			}
		});
		if (conversionMap.size || cspCount) {
			const modifierListClone = cloneModifierListNode(modifierList);
			modifierListClone.children = modifierListClone.children.map((modifierNode, index) => {
				const conversionRecord = conversionMap.get(index);
				if (conversionRecord) return conversionRecord;
				return modifierNode;
			}).flat();
			if (cspCount) {
				const cspValues = [];
				modifierListClone.children = modifierListClone.children.filter((modifierNode) => {
					if (modifierNode.name.value === CSP_MODIFIER) {
						if (!modifierNode.value?.value) throw new RuleConversionError("$csp modifier value is missing");
						cspValues.push(modifierNode.value?.value);
						return false;
					}
					return true;
				});
				modifierListClone.children.push(createModifierNode(CSP_MODIFIER, cspValues.join(CSP_SEPARATOR)));
			}
			modifierListClone.children = modifierListClone.children.filter((modifierNode, index, self) => self.findIndex((m) => m.name.value === modifierNode.name.value && m.exception === modifierNode.exception && m.value?.value === modifierNode.value?.value) === index);
			return createConversionResult(modifierListClone, true);
		}
		return createConversionResult(modifierList, false);
	}
	/**
	* Converts a network rule modifier list to uBlock format, if possible.
	*
	* @param modifierList Network rule modifier list node to convert
	* @param isException If `true`, the rule is an exception rule
	* @returns An object which follows the {@link ConversionResult} interface. Its `result` property contains
	* the converted node, and its `isConverted` flag indicates whether the original node was converted.
	* If the node was not converted, the result will contain the original node with the same object reference
	* @throws If the conversion is not possible
	*/
	static convertToUbo(modifierList, isException = false) {
		const conversionMap = new MultiValueMap();
		const resourceTypeModifiersToAdd = /* @__PURE__ */ new Set();
		modifierList.children.forEach((modifierNode, index) => {
			const originalModifierName = modifierNode.name.value;
			const modifierData = modifiersCompatibilityTable.getFirst(originalModifierName, GenericPlatform.UboAny);
			if (REDIRECT_MODIFIERS.has(originalModifierName)) {
				if (modifierNode.exception === true) throw new RuleConversionError(`Modifier '${modifierNode.name.value}' cannot be negated`);
				const redirectResourceName = modifierNode.value?.value;
				if (!redirectResourceName && !isException) throw new RuleConversionError(`No redirect resource specified for '${modifierNode.name.value}' modifier`);
				if (!redirectResourceName) return;
				const modifierName = modifierNode.name.value === ABP_REWRITE_MODIFIER ? REDIRECT_MODIFIER : modifierNode.name.value;
				const convertedRedirectResourceData = redirectsCompatibilityTable.getFirst(redirectResourceName, GenericPlatform.UboAny);
				const convertedRedirectResourceName = convertedRedirectResourceData?.name ?? redirectResourceName;
				if (convertedRedirectResourceData?.resourceTypes?.length) {
					const uboResourceTypeModifiers = redirectsCompatibilityTable.getResourceTypeModifiers(convertedRedirectResourceData, GenericPlatform.UboAny);
					const isNoopTextResource = convertedRedirectResourceName === UBO_NOOP_TEXT_RESOURCE;
					const hasValidResourceType = modifierList.children.some((modifier) => {
						const name = modifier.name.value;
						if (!isValidResourceType(name)) return false;
						const convertedModifierData = modifiersCompatibilityTable.getFirst(name, GenericPlatform.UboAny);
						return uboResourceTypeModifiers.has(convertedModifierData?.name ?? name);
					});
					if (!isNoopTextResource || !hasValidResourceType) uboResourceTypeModifiers.forEach((resourceType) => {
						resourceTypeModifiersToAdd.add(resourceType);
					});
				}
				if (modifierName !== originalModifierName || !isUndefined(convertedRedirectResourceName) && convertedRedirectResourceName !== redirectResourceName) conversionMap.add(index, createModifierNode(modifierName, convertedRedirectResourceName || redirectResourceName, modifierNode.exception));
				return;
			}
			if (modifierData && modifierData.name !== originalModifierName) conversionMap.add(index, createModifierNode(modifierData.name, modifierNode.value?.value, modifierNode.exception));
		});
		if (conversionMap.size || resourceTypeModifiersToAdd.size) {
			const modifierListClone = cloneModifierListNode(modifierList);
			modifierListClone.children = modifierListClone.children.map((modifierNode, index) => {
				const conversionRecord = conversionMap.get(index);
				if (conversionRecord) return conversionRecord;
				return modifierNode;
			}).flat();
			modifierListClone.children = modifierListClone.children.filter((modifierNode, index, self) => self.findIndex((m) => m.name.value === modifierNode.name.value && m.exception === modifierNode.exception && m.value?.value === modifierNode.value?.value) === index);
			if (resourceTypeModifiersToAdd.size) {
				const modifierNameSet = new Set(modifierList.children.map((m) => m.name.value));
				resourceTypeModifiersToAdd.forEach((resourceType) => {
					if (!modifierNameSet.has(resourceType)) modifierListClone.children.push(createModifierNode(resourceType));
				});
			}
			return createConversionResult(modifierListClone, true);
		}
		return createConversionResult(modifierList, false);
	}
};
//#endregion
export { NetworkRuleModifierListConverter };
