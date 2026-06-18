import { CompatibilityTableBase } from "./base.js";
import { modifiersCompatibilityTableData } from "./compatibility-table-data.js";
import { deepFreeze } from "../utils/deep-freeze.js";
import { isValidNoopModifier } from "../utils/noop-modifier.js";
//#region node_modules/@adguard/agtree/dist/compatibility-tables/modifiers.js
/**
* @file Compatibility tables for modifiers.
*/
/**
* Transforms the name of the modifier to a normalized form.
* This is a special case: the noop modifier normally '_', but it can consist of any number of characters,
* e.g. '____' is also valid. In this case, we need to normalize the name to '_'.
*
* @param name Modifier name to normalize.
* @returns Normalized modifier name.
*/
var noopModifierNameNormalizer = (name) => {
	if (name.startsWith("_")) {
		if (isValidNoopModifier(name)) return "_";
	}
	return name;
};
/**
* Compatibility table for modifiers.
*/
var ModifiersCompatibilityTable = class extends CompatibilityTableBase {
	/**
	* Creates a new instance of the compatibility table for modifiers.
	*
	* @param data Compatibility table data.
	*/
	constructor(data) {
		super(data, noopModifierNameNormalizer);
	}
};
/**
* Deep freeze the compatibility table data to avoid accidental modifications.
*/
deepFreeze(modifiersCompatibilityTableData);
/**
* Compatibility table instance for modifiers.
*/
var modifiersCompatibilityTable = new ModifiersCompatibilityTable(modifiersCompatibilityTableData);
//#endregion
export { modifiersCompatibilityTable };
