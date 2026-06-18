import { RECURSIVE_PSEUDO_CLASSES, tokenize } from "./parse.js";
//#region node_modules/@ghostery/adblocker-extended-selectors/dist/esm/extended.js
/*!
* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/
var EXTENDED_PSEUDO_CLASSES = new Set([
	"has-text",
	"matches-path",
	"matches-attr",
	"matches-css",
	"matches-css-after",
	"matches-css-before",
	"upward",
	"xpath"
]);
var PSEUDO_CLASSES = new Set([
	"active",
	"any",
	"any-link",
	"blank",
	"checked",
	"default",
	"defined",
	"dir",
	"disabled",
	"empty",
	"enabled",
	"first",
	"first-child",
	"first-of-type",
	"focus",
	"focus-visible",
	"focus-within",
	"fullscreen",
	"has",
	"host",
	"host-context",
	"hover",
	"in-range",
	"indeterminate",
	"invalid",
	"is",
	"lang",
	"last-child",
	"last-of-type",
	"left",
	"link",
	"matches",
	"not",
	"nth-child",
	"nth-last-child",
	"nth-last-of-type",
	"nth-of-type",
	"only-child",
	"only-of-type",
	"optional",
	"out-of-range",
	"placeholder-shown",
	"read-only",
	"read-write",
	"required",
	"right",
	"root",
	"scope",
	"target",
	"valid",
	"visited",
	"where"
]);
var PSEUDO_ELEMENTS = new Set([
	"after",
	"before",
	"first-letter",
	"first-line"
]);
var PSEUDO_DIRECTIVES = new Set([
	"remove",
	"remove-attr",
	"remove-class"
]);
var SelectorType;
(function(SelectorType) {
	SelectorType[SelectorType["Normal"] = 0] = "Normal";
	SelectorType[SelectorType["Extended"] = 1] = "Extended";
	SelectorType[SelectorType["Invalid"] = 2] = "Invalid";
})(SelectorType || (SelectorType = {}));
function classifySelector(selector) {
	if (selector.indexOf(":") === -1) return SelectorType.Normal;
	const tokens = tokenize(selector);
	let foundSupportedExtendedSelector = false;
	for (const token of tokens) if (token.type === "pseudo-class") {
		const { name } = token;
		if (EXTENDED_PSEUDO_CLASSES.has(name) === true || PSEUDO_DIRECTIVES.has(name) === true) foundSupportedExtendedSelector = true;
		else if (PSEUDO_CLASSES.has(name) === false && PSEUDO_ELEMENTS.has(name) === false) return SelectorType.Invalid;
		if (name === "has" && token.argument !== void 0 && token.argument.indexOf(":has(") !== -1) foundSupportedExtendedSelector = true;
		if (foundSupportedExtendedSelector === false && token.argument !== void 0 && RECURSIVE_PSEUDO_CLASSES.has(name) === true) {
			const argumentType = classifySelector(token.argument);
			if (argumentType === SelectorType.Invalid) return argumentType;
			else if (argumentType === SelectorType.Extended) foundSupportedExtendedSelector = true;
		}
	}
	if (foundSupportedExtendedSelector === true) return SelectorType.Extended;
	return SelectorType.Normal;
}
/**
* Exposes ASTs per purpose. For an instance, it distinguishes
* a directive selector from element selectors.
* @returns "element" AST and "directive" AST; no "element" AST
* means there's no selector, no "directive" AST means there's no
* pseudo-directive.
*/
function destructAST(ast) {
	if (ast.type === "compound") {
		const last = ast.compound[ast.compound.length - 1];
		if (last.type === "pseudo-class" && PSEUDO_DIRECTIVES.has(last.name)) {
			if (ast.compound.length < 3) return {
				element: ast.compound[0],
				directive: last
			};
			return {
				element: {
					type: "compound",
					compound: ast.compound.slice(0, -1)
				},
				directive: last
			};
		}
	}
	return {
		element: ast,
		directive: null
	};
}
/**
* Finds a position of a pseudo directive from the complete CSS
* selector. You can split the selector into normal or extended
* selector and pseudo directive using this function.
* @returns The position of a pseudo directive, or -1
*/
function indexOfPseudoDirective(selector) {
	let i = selector.lastIndexOf(")");
	if (i === -1) return -1;
	while (i--) {
		const c = selector.charCodeAt(i);
		if (c < 33) continue;
		if (c === 39 || c === 34 || c === 96) {
			while (i--) if (selector.charCodeAt(i) === c) break;
			break;
		}
	}
	if (i < 0) i = selector.length;
	while (i--) if (selector.charCodeAt(i) === 40) break;
	const lastIndexOfColon = selector.lastIndexOf(":", i);
	if (PSEUDO_DIRECTIVES.has(selector.slice(lastIndexOfColon + 1, i))) return lastIndexOfColon;
	return -1;
}
//#endregion
export { EXTENDED_PSEUDO_CLASSES, PSEUDO_CLASSES, PSEUDO_ELEMENTS, SelectorType, classifySelector, destructAST, indexOfPseudoDirective };
