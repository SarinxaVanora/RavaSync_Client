//#region node_modules/@adguard/agtree/dist/utils/deep-freeze.js
/**
* Simple deep freeze implementation.
* It freezes the object and all its properties recursively.
*
* @param object Object to freeze.
*
* @returns Frozen object.
*
* @template T Type of the object to freeze.
*
* @see {@link https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object/freeze#deep_freezing}
*/
var deepFreeze = (object) => {
	const propNames = Reflect.ownKeys(object);
	for (const name of propNames) {
		const value = object[name];
		if (value && typeof value === "object" || typeof value === "function") deepFreeze(value);
	}
	return Object.freeze(object);
};
//#endregion
export { deepFreeze };
