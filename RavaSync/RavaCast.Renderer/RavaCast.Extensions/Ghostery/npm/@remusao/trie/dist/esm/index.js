//#region node_modules/@remusao/trie/dist/esm/index.js
function newNode() {
	return {
		chars: /* @__PURE__ */ new Map(),
		code: void 0
	};
}
function create(strings) {
	const node = newNode();
	for (let i = 0; i < strings.length; i += 1) {
		const tok = strings[i];
		let root = node;
		for (let j = 0; j < tok.length; j += 1) {
			const c = tok.charCodeAt(j);
			let next = root.chars.get(c);
			if (next === void 0) {
				next = newNode();
				root.chars.set(c, next);
			}
			root = next;
		}
		root.code = i;
	}
	return node;
}
//#endregion
export { create };
