//#region node_modules/hybrids/src/template/utils.js
var metaMap = /* @__PURE__ */ new WeakMap();
function getMeta(key) {
	let value = metaMap.get(key);
	if (value) return value;
	metaMap.set(key, value = {});
	return value;
}
function getTemplateEnd(node) {
	let meta;
	while (node && (meta = getMeta(node)) && meta.endNode) node = meta.endNode;
	return node;
}
function removeTemplate(target) {
	if (target.nodeType === globalThis.Node.TEXT_NODE) {
		const data = metaMap.get(target);
		if (data && data.startNode) {
			const endNode = getTemplateEnd(data.endNode);
			let node = data.startNode;
			const lastNextSibling = endNode.nextSibling;
			while (node) {
				const nextSibling = node.nextSibling;
				node.parentNode.removeChild(node);
				node = nextSibling !== lastNextSibling && nextSibling;
			}
			metaMap.set(target, {});
		}
	} else {
		let child = target.childNodes[0];
		while (child) {
			target.removeChild(child);
			child = target.childNodes[0];
		}
		metaMap.set(target, {});
	}
}
var TIMESTAMP = Date.now();
var getPlaceholder = (id = 0) => `H-${TIMESTAMP}-${id}`;
//#endregion
export { getMeta, getPlaceholder, getTemplateEnd, removeTemplate };
