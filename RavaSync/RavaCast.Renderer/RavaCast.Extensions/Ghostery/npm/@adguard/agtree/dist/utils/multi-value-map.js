import { isUndefined } from "./type-guards.js";
//#region node_modules/@adguard/agtree/dist/utils/multi-value-map.js
/**
* A very simple map extension that allows to store multiple values for the same key
* by storing them in an array.
*
* @todo Add more methods if needed
*/
var MultiValueMap = class extends Map {
	/**
	* Adds a value to the map. If the key already exists, the value will be appended to the existing array,
	* otherwise a new array will be created for the key.
	*
	* @param key Key to add
	* @param values Value(s) to add
	*/
	add(key, ...values) {
		let currentValues = super.get(key);
		if (isUndefined(currentValues)) {
			currentValues = [];
			super.set(key, values);
		}
		currentValues.push(...values);
	}
};
//#endregion
export { MultiValueMap };
