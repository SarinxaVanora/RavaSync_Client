import { getMeta, removeTemplate } from "../utils.js";
//#region node_modules/hybrids/src/template/resolvers/node.js
function resolveNode(host, target, value) {
	removeTemplate(target);
	const meta = getMeta(target);
	meta.startNode = meta.endNode = value;
	target.parentNode.insertBefore(value, target.nextSibling);
}
//#endregion
export { resolveNode as default };
