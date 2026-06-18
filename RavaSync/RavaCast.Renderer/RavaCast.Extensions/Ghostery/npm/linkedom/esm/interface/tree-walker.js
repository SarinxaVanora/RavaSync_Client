import { END, NEXT, PRIVATE } from "../shared/symbols.js";
//#region node_modules/linkedom/esm/interface/tree-walker.js
var isOK = ({ nodeType }, mask) => {
	switch (nodeType) {
		case 1: return mask & 1;
		case 3: return mask & 4;
		case 8: return mask & 128;
		case 4: return mask & 8;
	}
	return 0;
};
/**
* @implements globalThis.TreeWalker
*/
var TreeWalker = class {
	constructor(root, whatToShow = -1) {
		this.root = root;
		this.currentNode = root;
		this.whatToShow = whatToShow;
		let { [NEXT]: next, [END]: end } = root;
		if (root.nodeType === 9) {
			const { documentElement } = root;
			next = documentElement;
			end = documentElement[END];
		}
		const nodes = [];
		while (next && next !== end) {
			if (isOK(next, whatToShow)) nodes.push(next);
			next = next[NEXT];
		}
		this[PRIVATE] = {
			i: 0,
			nodes
		};
	}
	nextNode() {
		const $ = this[PRIVATE];
		this.currentNode = $.i < $.nodes.length ? $.nodes[$.i++] : null;
		return this.currentNode;
	}
};
//#endregion
export { TreeWalker };
