//#region node_modules/@adguard/agtree/dist/utils/noop-modifier.js
/**
* Validates the noop modifier (i.e. only underscores).
*
* @param value Value of the modifier.
*
* @returns True if the modifier is valid, false otherwise.
*/
var isValidNoopModifier = (value) => {
	const { length } = value;
	if (length === 0) return false;
	for (let i = 0; i < length; i += 1) if (value[i] !== "_") return false;
	return true;
};
//#endregion
export { isValidNoopModifier };
