import { AttributeAction, SelectorType } from "../../../css-what/lib/es/types.js";
//#region node_modules/css-select/lib/esm/sort.js
var procedure = new Map([
	[SelectorType.Universal, 50],
	[SelectorType.Tag, 30],
	[SelectorType.Attribute, 1],
	[SelectorType.Pseudo, 0]
]);
function isTraversal(token) {
	return !procedure.has(token.type);
}
var attributes = new Map([
	[AttributeAction.Exists, 10],
	[AttributeAction.Equals, 8],
	[AttributeAction.Not, 7],
	[AttributeAction.Start, 6],
	[AttributeAction.End, 6],
	[AttributeAction.Any, 5]
]);
/**
* Sort the parts of the passed selector,
* as there is potential for optimization
* (some types of selectors are faster than others)
*
* @param arr Selector to sort
*/
function sortByProcedure(arr) {
	const procs = arr.map(getProcedure);
	for (let i = 1; i < arr.length; i++) {
		const procNew = procs[i];
		if (procNew < 0) continue;
		for (let j = i - 1; j >= 0 && procNew < procs[j]; j--) {
			const token = arr[j + 1];
			arr[j + 1] = arr[j];
			arr[j] = token;
			procs[j + 1] = procs[j];
			procs[j] = procNew;
		}
	}
}
function getProcedure(token) {
	var _a, _b;
	let proc = (_a = procedure.get(token.type)) !== null && _a !== void 0 ? _a : -1;
	if (token.type === SelectorType.Attribute) {
		proc = (_b = attributes.get(token.action)) !== null && _b !== void 0 ? _b : 4;
		if (token.action === AttributeAction.Equals && token.name === "id") proc = 9;
		if (token.ignoreCase) proc >>= 1;
	} else if (token.type === SelectorType.Pseudo) if (!token.data) proc = 3;
	else if (token.name === "has" || token.name === "contains") proc = 0;
	else if (Array.isArray(token.data)) {
		proc = Math.min(...token.data.map((d) => Math.min(...d.map(getProcedure))));
		if (proc < 0) proc = 0;
	} else proc = 2;
	return proc;
}
//#endregion
export { sortByProcedure as default, isTraversal };
