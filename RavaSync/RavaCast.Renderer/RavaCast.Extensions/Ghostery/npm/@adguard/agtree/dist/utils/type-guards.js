//#region node_modules/@adguard/agtree/dist/utils/type-guards.js
/**
* Checks whether the given value is undefined.
*
* @param value Value to check.
*
* @returns `true` if the value is 'undefined', `false` otherwise.
*/
var isUndefined = (value) => {
	return typeof value === "undefined";
};
/**
* Checks whether the given value is null.
*
* @param value Value to check.
*
* @returns `true` if the value is 'null', `false` otherwise.
*/
var isNull = (value) => {
	return value === null;
};
/**
* Checks whether the given value is a string.
*
* @param value Value to check.
* @returns `true` if the value is a string, `false` otherwise.
*/
var isString = (value) => {
	return typeof value === "string";
};
//#endregion
export { isNull, isString, isUndefined };
