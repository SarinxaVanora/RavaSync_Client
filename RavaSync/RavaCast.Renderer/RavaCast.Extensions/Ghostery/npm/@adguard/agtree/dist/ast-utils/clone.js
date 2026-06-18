import { isNull } from "../utils/type-guards.js";
//#region node_modules/@adguard/agtree/dist/ast-utils/clone.js
/**
* @file Custom clone functions for AST nodes, this is probably the most efficient way to clone AST nodes.
* @todo Maybe move them to parser classes as 'clone' methods
*/
/**
* Clones a scriptlet rule node.
*
* @param node Node to clone
* @returns Cloned node
*/
function cloneScriptletRuleNode(node) {
	return {
		type: node.type,
		children: node.children.map((child) => isNull(child) ? null : { ...child })
	};
}
/**
* Clones a domain list node.
*
* @param node Node to clone
* @returns Cloned node
*/
function cloneDomainListNode(node) {
	return {
		type: node.type,
		separator: node.separator,
		children: node.children.map((domain) => ({ ...domain }))
	};
}
/**
* Clones a modifier list node.
*
* @param node Node to clone
* @returns Cloned node
*/
function cloneModifierListNode(node) {
	return {
		type: node.type,
		children: node.children.map((modifier) => {
			const res = {
				type: modifier.type,
				exception: modifier.exception,
				name: { ...modifier.name }
			};
			if (modifier.value) res.value = { ...modifier.value };
			return res;
		})
	};
}
//#endregion
export { cloneDomainListNode, cloneModifierListNode, cloneScriptletRuleNode };
