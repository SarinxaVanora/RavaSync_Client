//#region node_modules/linkedom/esm/interface/node-list.js
/**
* @implements globalThis.NodeList
*/
var NodeList = class extends Array {
	item(i) {
		return i < this.length ? this[i] : null;
	}
};
//#endregion
export { NodeList };
