import { isNull, isString, isUndefined } from "../utils/type-guards.js";
import { CompatibilityTableBase } from "./base.js";
import { redirectsCompatibilityTableData } from "./compatibility-table-data.js";
import { deepFreeze } from "../utils/deep-freeze.js";
import { getResourceTypeModifier } from "./utils/resource-type-helpers.js";
//#region node_modules/@adguard/agtree/dist/compatibility-tables/redirects.js
/**
* @file Compatibility tables for redirects.
*/
/**
* Prefix for resource redirection names.
*/
var ABP_RESOURCE_PREFIX = "abp-resource:";
var ABP_RESOURCE_PREFIX_LENGTH = 13;
/**
* Normalizes the redirect name.
*
* @param name Redirect name to normalize.
*
* @returns Normalized redirect name.
*
* @example
* redirectNameNormalizer('abp-resource:my-resource') // => 'my-resource'
* redirectNameNormalizer('noop.js:99') // => 'noop.js'
*/
var redirectNameNormalizer = (name) => {
	if (name.startsWith(ABP_RESOURCE_PREFIX)) return name.slice(ABP_RESOURCE_PREFIX_LENGTH);
	/**
	* Remove :[integer] priority suffix from the name, if present.
	*
	* Note: negative values are also supported, see AG-48788.
	*
	* @see https://github.com/AdguardTeam/tsurlfilter/issues/59
	* @see https://github.com/gorhill/uBlock/wiki/Static-filter-syntax#redirect
	*/
	const colonIndex = name.lastIndexOf(":");
	if (colonIndex !== -1 && /^-?\d+$/.test(name.slice(colonIndex + 1))) return name.slice(0, colonIndex);
	return name;
};
/**
* Compatibility table for redirects.
*/
var RedirectsCompatibilityTable = class extends CompatibilityTableBase {
	/**
	* Creates a new instance of the compatibility table for redirects.
	*
	* @param data Compatibility table data.
	*/
	constructor(data) {
		super(data, redirectNameNormalizer);
	}
	/**
	* Gets the resource type adblock modifiers for the redirect for the given platform
	* based on the `resourceTypes` field.
	*
	* @param redirect Redirect name or redirect data.
	* @param platform Platform to get the modifiers for (can be specific, generic, or combined platforms).
	*
	* @returns Set of resource type modifiers or an empty set if the redirect is not found or has no resource types.
	*/
	getResourceTypeModifiers(redirect, platform) {
		let redirectData = null;
		if (isString(redirect)) redirectData = this.getFirst(redirect, platform);
		else redirectData = redirect;
		const modifierNames = /* @__PURE__ */ new Set();
		if (isNull(redirectData) || isUndefined(redirectData.resourceTypes)) return modifierNames;
		for (const resourceType of redirectData.resourceTypes) {
			const modifierName = getResourceTypeModifier(resourceType, platform);
			if (isNull(modifierName)) continue;
			modifierNames.add(modifierName);
		}
		return modifierNames;
	}
};
/**
* Deep freeze the compatibility table data to avoid accidental modifications.
*/
deepFreeze(redirectsCompatibilityTableData);
/**
* Compatibility table instance for redirects.
*/
var redirectsCompatibilityTable = new RedirectsCompatibilityTable(redirectsCompatibilityTableData);
//#endregion
export { redirectsCompatibilityTable };
