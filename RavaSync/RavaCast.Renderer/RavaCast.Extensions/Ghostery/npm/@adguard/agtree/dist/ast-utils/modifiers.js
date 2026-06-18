import { isUndefined } from "../utils/type-guards.js";
import { clone } from "../utils/clone.js";
//#region node_modules/@adguard/agtree/dist/ast-utils/modifiers.js
/**
* @file Utility functions for working with modifier nodes
*/
/**
* Creates a modifier node
*
* @param name Name of the modifier
* @param value Value of the modifier
* @param exception Whether the modifier is an exception
* @returns Modifier node
*/
function createModifierNode(name, value = void 0, exception = false) {
	const result = {
		type: "Modifier",
		exception,
		name: {
			type: "Value",
			value: name
		}
	};
	if (!isUndefined(value)) result.value = {
		type: "Value",
		value
	};
	return result;
}
/**
* Creates a modifier list node
*
* @param modifiers Modifiers to put in the list (optional, defaults to an empty list)
* @returns Modifier list node
*/
function createModifierListNode(modifiers = []) {
	return {
		type: "ModifierList",
		children: modifiers.length ? clone(modifiers) : []
	};
}
//#endregion
export { createModifierListNode, createModifierNode };
