import { AttributeAction, SelectorType } from "./types.js";
//#region node_modules/css-what/lib/es/parse.js
var reName = /^[^\\#]?(?:\\(?:[\da-f]{1,6}\s?|.)|[\w\-\u00b0-\uFFFF])+/;
var reEscape = /\\([\da-f]{1,6}\s?|(\s)|.)/gi;
var actionTypes = new Map([
	[126, AttributeAction.Element],
	[94, AttributeAction.Start],
	[36, AttributeAction.End],
	[42, AttributeAction.Any],
	[33, AttributeAction.Not],
	[124, AttributeAction.Hyphen]
]);
var unpackPseudos = new Set([
	"has",
	"not",
	"matches",
	"is",
	"where",
	"host",
	"host-context"
]);
/**
* Checks whether a specific selector is a traversal.
* This is useful eg. in swapping the order of elements that
* are not traversals.
*
* @param selector Selector to check.
*/
function isTraversal(selector) {
	switch (selector.type) {
		case SelectorType.Adjacent:
		case SelectorType.Child:
		case SelectorType.Descendant:
		case SelectorType.Parent:
		case SelectorType.Sibling:
		case SelectorType.ColumnCombinator: return true;
		default: return false;
	}
}
var stripQuotesFromPseudos = new Set(["contains", "icontains"]);
function funescape(_, escaped, escapedWhitespace) {
	const high = parseInt(escaped, 16) - 65536;
	return high !== high || escapedWhitespace ? escaped : high < 0 ? String.fromCharCode(high + 65536) : String.fromCharCode(high >> 10 | 55296, high & 1023 | 56320);
}
function unescapeCSS(str) {
	return str.replace(reEscape, funescape);
}
function isQuote(c) {
	return c === 39 || c === 34;
}
function isWhitespace(c) {
	return c === 32 || c === 9 || c === 10 || c === 12 || c === 13;
}
/**
* Parses `selector`, optionally with the passed `options`.
*
* @param selector Selector to parse.
* @param options Options for parsing.
* @returns Returns a two-dimensional array.
* The first dimension represents selectors separated by commas (eg. `sub1, sub2`),
* the second contains the relevant tokens for that selector.
*/
function parse(selector) {
	const subselects = [];
	const endIndex = parseSelector(subselects, `${selector}`, 0);
	if (endIndex < selector.length) throw new Error(`Unmatched selector: ${selector.slice(endIndex)}`);
	return subselects;
}
function parseSelector(subselects, selector, selectorIndex) {
	let tokens = [];
	function getName(offset) {
		const match = selector.slice(selectorIndex + offset).match(reName);
		if (!match) throw new Error(`Expected name, found ${selector.slice(selectorIndex)}`);
		const [name] = match;
		selectorIndex += offset + name.length;
		return unescapeCSS(name);
	}
	function stripWhitespace(offset) {
		selectorIndex += offset;
		while (selectorIndex < selector.length && isWhitespace(selector.charCodeAt(selectorIndex))) selectorIndex++;
	}
	function readValueWithParenthesis() {
		selectorIndex += 1;
		const start = selectorIndex;
		let counter = 1;
		for (; counter > 0 && selectorIndex < selector.length; selectorIndex++) if (selector.charCodeAt(selectorIndex) === 40 && !isEscaped(selectorIndex)) counter++;
		else if (selector.charCodeAt(selectorIndex) === 41 && !isEscaped(selectorIndex)) counter--;
		if (counter) throw new Error("Parenthesis not matched");
		return unescapeCSS(selector.slice(start, selectorIndex - 1));
	}
	function isEscaped(pos) {
		let slashCount = 0;
		while (selector.charCodeAt(--pos) === 92) slashCount++;
		return (slashCount & 1) === 1;
	}
	function ensureNotTraversal() {
		if (tokens.length > 0 && isTraversal(tokens[tokens.length - 1])) throw new Error("Did not expect successive traversals.");
	}
	function addTraversal(type) {
		if (tokens.length > 0 && tokens[tokens.length - 1].type === SelectorType.Descendant) {
			tokens[tokens.length - 1].type = type;
			return;
		}
		ensureNotTraversal();
		tokens.push({ type });
	}
	function addSpecialAttribute(name, action) {
		tokens.push({
			type: SelectorType.Attribute,
			name,
			action,
			value: getName(1),
			namespace: null,
			ignoreCase: "quirks"
		});
	}
	/**
	* We have finished parsing the current part of the selector.
	*
	* Remove descendant tokens at the end if they exist,
	* and return the last index, so that parsing can be
	* picked up from here.
	*/
	function finalizeSubselector() {
		if (tokens.length && tokens[tokens.length - 1].type === SelectorType.Descendant) tokens.pop();
		if (tokens.length === 0) throw new Error("Empty sub-selector");
		subselects.push(tokens);
	}
	stripWhitespace(0);
	if (selector.length === selectorIndex) return selectorIndex;
	loop: while (selectorIndex < selector.length) {
		const firstChar = selector.charCodeAt(selectorIndex);
		switch (firstChar) {
			case 32:
			case 9:
			case 10:
			case 12:
			case 13:
				if (tokens.length === 0 || tokens[0].type !== SelectorType.Descendant) {
					ensureNotTraversal();
					tokens.push({ type: SelectorType.Descendant });
				}
				stripWhitespace(1);
				break;
			case 62:
				addTraversal(SelectorType.Child);
				stripWhitespace(1);
				break;
			case 60:
				addTraversal(SelectorType.Parent);
				stripWhitespace(1);
				break;
			case 126:
				addTraversal(SelectorType.Sibling);
				stripWhitespace(1);
				break;
			case 43:
				addTraversal(SelectorType.Adjacent);
				stripWhitespace(1);
				break;
			case 46:
				addSpecialAttribute("class", AttributeAction.Element);
				break;
			case 35:
				addSpecialAttribute("id", AttributeAction.Equals);
				break;
			case 91: {
				stripWhitespace(1);
				let name;
				let namespace = null;
				if (selector.charCodeAt(selectorIndex) === 124) name = getName(1);
				else if (selector.startsWith("*|", selectorIndex)) {
					namespace = "*";
					name = getName(2);
				} else {
					name = getName(0);
					if (selector.charCodeAt(selectorIndex) === 124 && selector.charCodeAt(selectorIndex + 1) !== 61) {
						namespace = name;
						name = getName(1);
					}
				}
				stripWhitespace(0);
				let action = AttributeAction.Exists;
				const possibleAction = actionTypes.get(selector.charCodeAt(selectorIndex));
				if (possibleAction) {
					action = possibleAction;
					if (selector.charCodeAt(selectorIndex + 1) !== 61) throw new Error("Expected `=`");
					stripWhitespace(2);
				} else if (selector.charCodeAt(selectorIndex) === 61) {
					action = AttributeAction.Equals;
					stripWhitespace(1);
				}
				let value = "";
				let ignoreCase = null;
				if (action !== "exists") {
					if (isQuote(selector.charCodeAt(selectorIndex))) {
						const quote = selector.charCodeAt(selectorIndex);
						let sectionEnd = selectorIndex + 1;
						while (sectionEnd < selector.length && (selector.charCodeAt(sectionEnd) !== quote || isEscaped(sectionEnd))) sectionEnd += 1;
						if (selector.charCodeAt(sectionEnd) !== quote) throw new Error("Attribute value didn't end");
						value = unescapeCSS(selector.slice(selectorIndex + 1, sectionEnd));
						selectorIndex = sectionEnd + 1;
					} else {
						const valueStart = selectorIndex;
						while (selectorIndex < selector.length && (!isWhitespace(selector.charCodeAt(selectorIndex)) && selector.charCodeAt(selectorIndex) !== 93 || isEscaped(selectorIndex))) selectorIndex += 1;
						value = unescapeCSS(selector.slice(valueStart, selectorIndex));
					}
					stripWhitespace(0);
					const forceIgnore = selector.charCodeAt(selectorIndex) | 32;
					if (forceIgnore === 115) {
						ignoreCase = false;
						stripWhitespace(1);
					} else if (forceIgnore === 105) {
						ignoreCase = true;
						stripWhitespace(1);
					}
				}
				if (selector.charCodeAt(selectorIndex) !== 93) throw new Error("Attribute selector didn't terminate");
				selectorIndex += 1;
				const attributeSelector = {
					type: SelectorType.Attribute,
					name,
					action,
					value,
					namespace,
					ignoreCase
				};
				tokens.push(attributeSelector);
				break;
			}
			case 58: {
				if (selector.charCodeAt(selectorIndex + 1) === 58) {
					tokens.push({
						type: SelectorType.PseudoElement,
						name: getName(2).toLowerCase(),
						data: selector.charCodeAt(selectorIndex) === 40 ? readValueWithParenthesis() : null
					});
					continue;
				}
				const name = getName(1).toLowerCase();
				let data = null;
				if (selector.charCodeAt(selectorIndex) === 40) if (unpackPseudos.has(name)) {
					if (isQuote(selector.charCodeAt(selectorIndex + 1))) throw new Error(`Pseudo-selector ${name} cannot be quoted`);
					data = [];
					selectorIndex = parseSelector(data, selector, selectorIndex + 1);
					if (selector.charCodeAt(selectorIndex) !== 41) throw new Error(`Missing closing parenthesis in :${name} (${selector})`);
					selectorIndex += 1;
				} else {
					data = readValueWithParenthesis();
					if (stripQuotesFromPseudos.has(name)) {
						const quot = data.charCodeAt(0);
						if (quot === data.charCodeAt(data.length - 1) && isQuote(quot)) data = data.slice(1, -1);
					}
					data = unescapeCSS(data);
				}
				tokens.push({
					type: SelectorType.Pseudo,
					name,
					data
				});
				break;
			}
			case 44:
				finalizeSubselector();
				tokens = [];
				stripWhitespace(1);
				break;
			default: {
				if (selector.startsWith("/*", selectorIndex)) {
					const endIndex = selector.indexOf("*/", selectorIndex + 2);
					if (endIndex < 0) throw new Error("Comment was not terminated");
					selectorIndex = endIndex + 2;
					if (tokens.length === 0) stripWhitespace(0);
					break;
				}
				let namespace = null;
				let name;
				if (firstChar === 42) {
					selectorIndex += 1;
					name = "*";
				} else if (firstChar === 124) {
					name = "";
					if (selector.charCodeAt(selectorIndex + 1) === 124) {
						addTraversal(SelectorType.ColumnCombinator);
						stripWhitespace(2);
						break;
					}
				} else if (reName.test(selector.slice(selectorIndex))) name = getName(0);
				else break loop;
				if (selector.charCodeAt(selectorIndex) === 124 && selector.charCodeAt(selectorIndex + 1) !== 124) {
					namespace = name;
					if (selector.charCodeAt(selectorIndex + 1) === 42) {
						name = "*";
						selectorIndex += 2;
					} else name = getName(1);
				}
				tokens.push(name === "*" ? {
					type: SelectorType.Universal,
					namespace
				} : {
					type: SelectorType.Tag,
					name,
					namespace
				});
			}
		}
	}
	finalizeSubselector();
	return selectorIndex;
}
//#endregion
export { parse };
