(function() {
	//#region node_modules/@ghostery/adblocker-content/dist/esm/index.js
	/*!
	* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
	*
	* This Source Code Form is subject to the terms of the Mozilla Public
	* License, v. 2.0. If a copy of the MPL was not distributed with this
	* file, You can obtain one at https://mozilla.org/MPL/2.0/.
	*/
	var SCRIPT_ID = "ghostery-adblocker-script";
	var IGNORED_TAGS = new Set([
		"br",
		"head",
		"link",
		"meta",
		"script",
		"style",
		"s"
	]);
	function debounce(fn, { waitFor, maxWait }) {
		let delayedTimer;
		let maxWaitTimer;
		const clear = () => {
			clearTimeout(delayedTimer);
			clearTimeout(maxWaitTimer);
			delayedTimer = void 0;
			maxWaitTimer = void 0;
		};
		const run = () => {
			clear();
			fn();
		};
		return [() => {
			if (maxWait > 0 && maxWaitTimer === void 0) maxWaitTimer = setTimeout(run, maxWait);
			clearTimeout(delayedTimer);
			delayedTimer = setTimeout(run, waitFor);
		}, clear];
	}
	function isElement(node) {
		return node.nodeType === 1;
	}
	function getElementsFromMutations(mutations) {
		const elements = [];
		for (const mutation of mutations) if (mutation.type === "attributes") {
			if (isElement(mutation.target)) elements.push(mutation.target);
		} else if (mutation.type === "childList") {
			for (const addedNode of mutation.addedNodes) if (isElement(addedNode) && addedNode.id !== SCRIPT_ID) elements.push(addedNode);
		}
		return elements;
	}
	/**
	* WARNING: this function should be self-contained and not rely on any global
	* symbol. That constraint needs to be fulfilled because this function can
	* potentially be injected in content-script (e.g.: see PuppeteerBlocker for
	* more details).
	*/
	function extractFeaturesFromDOM(roots = [document.documentElement]) {
		const ignoredTags = new Set([
			"br",
			"head",
			"link",
			"meta",
			"script",
			"style",
			"s"
		]);
		const classes = /* @__PURE__ */ new Set();
		const hrefs = /* @__PURE__ */ new Set();
		const ids = /* @__PURE__ */ new Set();
		const seenElements = /* @__PURE__ */ new Set();
		for (const root of roots) for (const element of [root, ...root.querySelectorAll("[id]:not(html):not(body),[class]:not(html):not(body),[href]:not(html):not(body)")]) {
			if (seenElements.has(element)) continue;
			seenElements.add(element);
			if (ignoredTags.has(element.nodeName.toLowerCase())) continue;
			const id = element.getAttribute("id");
			if (typeof id === "string") ids.add(id);
			const classList = element.classList;
			for (const classEntry of classList) classes.add(classEntry);
			const href = element.getAttribute("href");
			if (typeof href === "string") hrefs.add(href);
		}
		return {
			classes: Array.from(classes),
			hrefs: Array.from(hrefs),
			ids: Array.from(ids)
		};
	}
	var DOMMonitor = class {
		constructor(cb) {
			this.cb = cb;
			this.knownIds = /* @__PURE__ */ new Set();
			this.knownHrefs = /* @__PURE__ */ new Set();
			this.knownClasses = /* @__PURE__ */ new Set();
			this.observer = null;
		}
		queryAll(window) {
			this.cb({
				type: "elements",
				elements: [window.document.documentElement]
			});
			this.handleUpdatedNodes([window.document.documentElement]);
		}
		start(window) {
			if (this.observer === null && window.MutationObserver !== void 0) {
				const nodes = /* @__PURE__ */ new Set();
				const handleUpdatedNodesCallback = () => {
					this.handleUpdatedNodes(Array.from(nodes));
					nodes.clear();
				};
				const [debouncedHandleUpdatedNodes, cancelHandleUpdatedNodes] = debounce(handleUpdatedNodesCallback, {
					waitFor: 25,
					maxWait: 1e3
				});
				this.observer = new window.MutationObserver((mutations) => {
					getElementsFromMutations(mutations).forEach(nodes.add, nodes);
					if (nodes.size > 512) {
						cancelHandleUpdatedNodes();
						handleUpdatedNodesCallback();
					} else debouncedHandleUpdatedNodes();
				});
				this.observer.observe(window.document.documentElement, {
					attributes: true,
					attributeFilter: [
						"class",
						"id",
						"href"
					],
					childList: true,
					subtree: true
				});
			}
		}
		stop() {
			if (this.observer !== null) {
				this.observer.disconnect();
				this.observer = null;
			}
		}
		handleNewFeatures({ hrefs, ids, classes }) {
			const newIds = [];
			const newClasses = [];
			const newHrefs = [];
			for (const id of ids) if (this.knownIds.has(id) === false) {
				newIds.push(id);
				this.knownIds.add(id);
			}
			for (const cls of classes) if (this.knownClasses.has(cls) === false) {
				newClasses.push(cls);
				this.knownClasses.add(cls);
			}
			for (const href of hrefs) if (this.knownHrefs.has(href) === false) {
				newHrefs.push(href);
				this.knownHrefs.add(href);
			}
			if (newIds.length !== 0 || newClasses.length !== 0 || newHrefs.length !== 0) {
				this.cb({
					type: "features",
					classes: newClasses,
					hrefs: newHrefs,
					ids: newIds
				});
				return true;
			}
			return false;
		}
		handleUpdatedNodes(elements) {
			if (elements.length !== 0) {
				this.cb({
					type: "elements",
					elements: elements.filter((e) => IGNORED_TAGS.has(e.nodeName.toLowerCase()) === false)
				});
				return this.handleNewFeatures(extractFeaturesFromDOM(elements));
			}
			return false;
		}
	};
	//#endregion
	//#region node_modules/@ghostery/adblocker-extended-selectors/dist/esm/parse.js
	/*!
	* Based on parsel. Extended by Rémi Berson for Ghostery (2021).
	* https://github.com/LeaVerou/parsel
	*
	* MIT License
	*
	* Copyright (c) 2020 Lea Verou
	*
	* Permission is hereby granted, free of charge, to any person obtaining a copy
	* of this software and associated documentation files (the "Software"), to deal
	* in the Software without restriction, including without limitation the rights
	* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	* copies of the Software, and to permit persons to whom the Software is
	* furnished to do so, subject to the following conditions:
	*
	* The above copyright notice and this permission notice shall be included in all
	* copies or substantial portions of the Software.
	*
	* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	* SOFTWARE.
	*/
	var TOKENS = {
		attribute: /\[\s*(?:(?<namespace>\*|[-\w]*)\|)?(?<name>[-\w\u{0080}-\u{FFFF}]+)\s*(?:(?<operator>\W?=)\s*(?<value>.+?)\s*(?<caseSensitive>[iIsS])?\s*)?\]/gu,
		id: /#(?<name>(?:[-\w\u{0080}-\u{FFFF}]|\\.)+)/gu,
		class: /\.(?<name>(?:[-\w\u{0080}-\u{FFFF}]|\\.)+)/gu,
		comma: /\s*,\s*/g,
		combinator: /\s*[\s>+~]\s*/g,
		"pseudo-element": /::(?<name>[-\w\u{0080}-\u{FFFF}]+)(?:\((?:¶*)\))?/gu,
		"pseudo-class": /:(?<name>[-\w\u{0080}-\u{FFFF}]+)(?:\((?<argument>¶*)\))?/gu,
		type: /(?:(?<namespace>\*|[-\w]*)\|)?(?<name>[-\w\u{0080}-\u{FFFF}]+)|\*/gu
	};
	new Set([...new Set(["pseudo-class", "pseudo-element"]), "attribute"]);
	var TOKENS_FOR_RESTORE = Object.assign({}, TOKENS);
	TOKENS_FOR_RESTORE["pseudo-element"] = RegExp(TOKENS["pseudo-element"].source.replace("(?<argument>¶*)", "(?<argument>.*?)"), "gu");
	TOKENS_FOR_RESTORE["pseudo-class"] = RegExp(TOKENS["pseudo-class"].source.replace("(?<argument>¶*)", "(?<argument>.*)"), "gu");
	//#endregion
	//#region node_modules/@ghostery/adblocker-extended-selectors/dist/esm/eval.js
	/*!
	* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
	*
	* This Source Code Form is subject to the terms of the Mozilla Public
	* License, v. 2.0. If a copy of the MPL was not distributed with this
	* file, You can obtain one at https://mozilla.org/MPL/2.0/.
	*/
	var createXpathExpression = (function() {
		const expressions = [];
		return function compile(query) {
			for (const [literal, expression] of expressions) if (query === literal) return expression;
			const expression = document.createExpression(query);
			expressions.push([query, expression]);
			return expression;
		};
	})();
	/**
	* Evaluates an XPath expression and returns matching Element nodes.
	* @param element - The context element for XPath evaluation
	* @param xpathExpression - The XPath expression to evaluate
	* @returns Array of Element nodes that match the XPath expression
	*/
	function handleXPathSelector(element, xpathExpression) {
		try {
			if (typeof Node === "undefined" || typeof XPathResult === "undefined") return [];
			const result = createXpathExpression(xpathExpression).evaluate(element, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE);
			if (result.resultType !== XPathResult.ORDERED_NODE_SNAPSHOT_TYPE) return [];
			const elements = [];
			for (let i = 0; i < result.snapshotLength; i++) {
				const node = result.snapshotItem(i);
				if (node?.nodeType === Node.ELEMENT_NODE) elements.push(node);
			}
			return elements;
		} catch (e) {
			return [];
		}
	}
	function parseCSSValue(cssValue) {
		const firstColonIndex = cssValue.indexOf(":");
		if (firstColonIndex === -1) throw new Error("Invalid CSS value format: no colon found");
		const property = cssValue.slice(0, firstColonIndex).trim();
		const value = cssValue.slice(firstColonIndex + 1).trim();
		return {
			property,
			value,
			isRegex: value.startsWith("/") && value.lastIndexOf("/") > 0
		};
	}
	function matchCSSProperty(element, cssValue, pseudoElement) {
		const { property, value, isRegex } = parseCSSValue(cssValue);
		const win = element.ownerDocument && element.ownerDocument.defaultView;
		if (!win) throw new Error("No window context for element");
		const actualValue = win.getComputedStyle(element, pseudoElement)[property];
		if (isRegex) return parseRegex(value).test(actualValue);
		return actualValue === value;
	}
	function parseRegex(str) {
		if (str.startsWith("/") && str.lastIndexOf("/") > 0) {
			const lastSlashIndex = str.lastIndexOf("/");
			const pattern = str.slice(1, lastSlashIndex);
			const flags = str.slice(lastSlashIndex + 1);
			if (flags.length > 3 || !/^[imu]*$/.test(flags)) throw new Error(`Invalid regex flags: ${flags}`);
			return new RegExp(pattern, flags);
		} else return new RegExp(str);
	}
	function stripsWrappingQuotes(str) {
		if (str.startsWith("\"") && str.endsWith("\"") || str.startsWith("'") && str.endsWith("'")) return str.slice(1, -1);
		return str;
	}
	function matchPattern(pattern, text) {
		pattern = stripsWrappingQuotes(pattern);
		if (pattern.startsWith("/") && (pattern.endsWith("/") || pattern.endsWith("/i"))) {
			let caseSensitive = true;
			pattern = pattern.slice(1);
			if (pattern.endsWith("/")) pattern = pattern.slice(0, -1);
			else {
				pattern = pattern.slice(0, -2);
				caseSensitive = false;
			}
			return new RegExp(pattern, caseSensitive === false ? "i" : void 0).test(text);
		}
		return text.includes(pattern);
	}
	/**
	* Checks if the given element complies with the given selector.
	* @param element The subjective element.
	* @param selector The selector.
	*/
	function matches(element, selector) {
		if (selector.type === "id" || selector.type === "class" || selector.type === "type" || selector.type === "attribute") return element.matches(selector.content);
		else if (selector.type === "list") return selector.list.some((s) => matches(element, s));
		else if (selector.type === "compound") return selector.compound.every((s) => matches(element, s));
		else if (selector.type === "pseudo-class") {
			if (selector.name === "has") return selector.subtree !== void 0 && querySelectorAll(element, selector.subtree).length !== 0;
			else if (selector.name === "not") return selector.subtree !== void 0 && traverse(element, [selector.subtree]).length === 0;
			else if (selector.name === "has-text") {
				const { argument } = selector;
				if (argument === void 0) return false;
				const text = element.textContent;
				if (text === null) return false;
				return matchPattern(argument, text.trim());
			} else if (selector.name === "min-text-length") {
				const minLength = Number(selector.argument);
				if (Number.isNaN(minLength) || minLength < 0) return false;
				const text = element.textContent;
				if (text === null) return false;
				return text.length >= minLength;
			} else if (selector.name === "matches-path") {
				const { argument } = selector;
				if (argument === void 0) return false;
				const window = element.ownerDocument?.defaultView;
				if (!window) return false;
				const fullUrl = window.location.pathname + window.location.search;
				try {
					return parseRegex(argument).test(fullUrl);
				} catch (e) {
					return fullUrl.includes(argument);
				}
			} else if (selector.name === "matches-attr") {
				const { argument } = selector;
				if (argument === void 0) return false;
				const indexOfEqual = argument.indexOf("=");
				let namePattern;
				let valuePattern;
				if (indexOfEqual === -1) namePattern = argument;
				else {
					namePattern = argument.slice(0, indexOfEqual);
					valuePattern = argument.slice(indexOfEqual + 1);
				}
				namePattern = stripsWrappingQuotes(namePattern);
				valuePattern = valuePattern ? stripsWrappingQuotes(valuePattern) : void 0;
				let valueRegex = null;
				if (valuePattern?.startsWith("/") && valuePattern.lastIndexOf("/") > 0) valueRegex = parseRegex(valuePattern);
				if (namePattern.startsWith("/") && namePattern.lastIndexOf("/") > 0) {
					const regex = parseRegex(namePattern);
					const matchingAttrs = [...element.attributes].filter((attr) => regex.test(attr.name));
					if (!valuePattern) return matchingAttrs.length > 0;
					return matchingAttrs.some((attr) => valueRegex ? valueRegex.test(attr.value) : attr.value === valuePattern);
				} else {
					const value = element.getAttribute(namePattern);
					if (value === null) return false;
					if (!valuePattern) return true;
					return valueRegex ? valueRegex.test(value) : value === valuePattern;
				}
			} else if (selector.name === "matches-css") return selector.argument !== void 0 && matchCSSProperty(element, selector.argument);
			else if (selector.name === "matches-css-after") return selector.argument !== void 0 && matchCSSProperty(element, selector.argument, "::after");
			else if (selector.name === "matches-css-before") return selector.argument !== void 0 && matchCSSProperty(element, selector.argument, "::before");
		}
		return false;
	}
	/**
	* Describes CSS combinator behaviors from the given element.
	* @param element The current subjective element.
	* @param selector A complex selector.
	*/
	function handleComplexSelector(element, selector) {
		const leftElements = selector.left === void 0 ? [element] : querySelectorAll(element, selector.left);
		const selectors = selector.right.type === "compound" ? selector.right.compound : [selector.right];
		const results = /* @__PURE__ */ new Set();
		switch (selector.combinator) {
			case " ":
				for (const leftElement of leftElements) for (const child of leftElement.querySelectorAll("*")) for (const result of traverse(child, selectors)) results.add(result);
				break;
			case ">":
				for (const leftElement of leftElements) for (const child of leftElement.children) for (const result of traverse(child, selectors)) results.add(result);
				break;
			case "~":
				for (const leftElement of leftElements) {
					let sibling = leftElement;
					while ((sibling = sibling.nextElementSibling) !== null) for (const result of traverse(sibling, selectors)) results.add(result);
				}
				break;
			case "+":
				for (const leftElement of leftElements) {
					if (leftElement.nextElementSibling === null) continue;
					for (const result of traverse(leftElement.nextElementSibling, selectors)) results.add(result);
				}
				break;
		}
		return Array.from(results);
	}
	/**
	* Transposes the given element with a selector.
	* @param element The subjective element
	* @param selector A selector
	* @returns An array of elements or null if not a transpose operator.
	*/
	function transpose(element, selector) {
		if (selector.type === "pseudo-class") {
			if (selector.name === "upward") {
				if (selector.argument === void 0) return [];
				const argument = stripsWrappingQuotes(selector.argument);
				let parentElement = element;
				let number = Number(argument);
				if (Number.isInteger(number)) {
					if (number <= 0 || number >= 256) return [];
					while ((parentElement = parentElement.parentElement) !== null) if (--number === 0) return [parentElement];
				} else while ((parentElement = parentElement.parentElement) !== null) if (parentElement.matches(argument)) return [parentElement];
				return [];
			} else if (selector.name === "xpath") {
				if (selector.argument === void 0) return [];
				return handleXPathSelector(element, selector.argument);
			}
		}
		return null;
	}
	/**
	* Checks elements by traversing from the given element.
	* You need to decide the subjective element candidates manually.
	* It doesn't look for the children of the given element.
	* @param root The subjective element.
	* @param selectors The selector list to validate with.
	* @returns If the given element and all followed candidate fails, it returns an empty array.
	*/
	function traverse(root, selectors) {
		if (selectors.length === 0) return [];
		const traversals = [{
			element: root,
			index: 0
		}];
		const results = [];
		while (traversals.length) {
			const traversal = traversals.pop();
			const { element } = traversal;
			let { index } = traversal;
			for (; index < selectors.length; index++) {
				const candidates = transpose(element, selectors[index]);
				if (candidates !== null) {
					traversals.push(...candidates.map((element) => ({
						element,
						index: index + 1
					})));
					break;
				} else if (matches(element, selectors[index]) === false) break;
			}
			if (index === selectors.length && !results.includes(element)) results.push(element);
		}
		return results;
	}
	/**
	* Check if the selector is delegating the traversal process to the external method.
	* @param selector The pseudo class selector
	*/
	function isDelegatedPseudoClass(selector) {
		if (selector.name === "xpath") return true;
		return false;
	}
	function querySelectorAll(element, selector) {
		if (selector.type === "id" || selector.type === "class" || selector.type === "type" || selector.type === "attribute") return Array.from(element.querySelectorAll(selector.content));
		if (selector.type === "list") {
			const results = [];
			for (const item of selector.list) for (const result of querySelectorAll(element, item)) if (!results.includes(result)) results.push(result);
			return results;
		}
		if (selector.type === "compound") {
			const results = [];
			const [first, ...rest] = selector.compound;
			for (const subjective of querySelectorAll(element, first)) for (const result of traverse(subjective, rest)) if (!results.includes(result)) results.push(result);
			return results;
		}
		if (selector.type === "complex") return handleComplexSelector(element, selector);
		if (selector.type === "pseudo-class") {
			const results = [];
			for (const subjective of isDelegatedPseudoClass(selector) ? [element] : element.querySelectorAll("*")) for (const result of traverse(subjective, [selector])) if (!results.includes(result)) results.push(result);
			return results;
		}
		return [];
	}
	/**
	* Executes pseudo-directive on the element.
	* @param element The target from normal or extended selector.
	* @param selector The AST only containing pseudo directive.
	*/
	function handlePseudoDirective(element, selector) {
		if (selector.type !== "pseudo-class") return;
		if (selector.name === "remove") element.remove();
		else if (selector.name === "remove-attr") if (selector.argument === void 0) return;
		else if (selector.argument.startsWith("/") && selector.argument.endsWith("/")) {
			const regex = parseRegex(selector.argument);
			for (let i = element.attributes.length - 1; i >= 0; i--) {
				const attribute = element.attributes.item(i);
				if (attribute !== null && regex.test(attribute.name)) element.removeAttribute(attribute.name);
			}
		} else return element.removeAttribute(stripsWrappingQuotes(selector.argument));
		else if (selector.name === "remove-class") if (selector.argument === void 0) return;
		else if (selector.argument.startsWith("/") && selector.argument.endsWith("/")) {
			const regex = parseRegex(selector.argument);
			for (let i = element.classList.length - 1; i >= 0; i--) {
				const className = element.classList.item(i);
				if (className !== null && regex.test(className)) element.classList.remove(className);
			}
		} else return element.classList.remove(stripsWrappingQuotes(selector.argument));
	}
	//#endregion
	//#region node_modules/@ghostery/adblocker-extended-selectors/dist/esm/extended.js
	/*!
	* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
	*
	* This Source Code Form is subject to the terms of the Mozilla Public
	* License, v. 2.0. If a copy of the MPL was not distributed with this
	* file, You can obtain one at https://mozilla.org/MPL/2.0/.
	*/
	var SelectorType;
	(function(SelectorType) {
		SelectorType[SelectorType["Normal"] = 0] = "Normal";
		SelectorType[SelectorType["Extended"] = 1] = "Extended";
		SelectorType[SelectorType["Invalid"] = 2] = "Invalid";
	})(SelectorType || (SelectorType = {}));
	//#endregion
	//#region node_modules/@ghostery/adblocker-extended-selectors/dist/esm/index.js
	/*!
	* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
	*
	* This Source Code Form is subject to the terms of the Mozilla Public
	* License, v. 2.0. If a copy of the MPL was not distributed with this
	* file, You can obtain one at https://mozilla.org/MPL/2.0/.
	*/
	//#endregion
	//#region src/content_scripts/adblocker/extended-selectors.js
	/**
	* Ghostery Browser Extension
	* https://www.ghostery.com/
	*
	* Copyright 2017-present Ghostery GmbH. All rights reserved.
	*
	* This Source Code Form is subject to the terms of the Mozilla Public
	* License, v. 2.0. If a copy of the MPL was not distributed with this
	* file, You can obtain one at http://mozilla.org/MPL/2.0
	*/
	var UPDATE_EXTENDED_TIMEOUT = null;
	var PENDING = /* @__PURE__ */ new Set();
	var EXTENDED = /* @__PURE__ */ new Map();
	var HIDDEN = /* @__PURE__ */ new Map();
	function cachedQuerySelector(root, selector, cache) {
		const cachedElements = cache.get(root)?.get(selector);
		if (cachedElements !== void 0) return cachedElements;
		const selected = new Set(querySelectorAll(root, selector.ast));
		if (selector.attribute !== void 0) {
			let cachedSelectors = cache.get(root);
			if (cachedSelectors === void 0) {
				cachedSelectors = /* @__PURE__ */ new Map();
				cache.set(root, cachedSelectors);
			}
			cachedSelectors.set(selector, selected);
		}
		return selected;
	}
	function updateExtended() {
		if (PENDING.size === 0 || EXTENDED.size === 0) return;
		const cache = /* @__PURE__ */ new Map();
		const elementsToHide = /* @__PURE__ */ new Map();
		const roots = [...PENDING].filter((e) => e.isConnected === true);
		PENDING.clear();
		for (const root of roots) for (const selector of EXTENDED.values()) for (const element of cachedQuerySelector(root, selector, cache)) if (selector.directive !== void 0) handlePseudoDirective(element, selector.directive);
		else if (selector.attribute !== void 0 && HIDDEN.has(element) === false) elementsToHide.set(element, {
			selector,
			root
		});
		for (const [element, { selector, root }] of elementsToHide.entries()) if (selector.attribute !== void 0) {
			element.setAttribute(selector.attribute, "");
			HIDDEN.set(element, {
				selector,
				root
			});
		}
		for (const [element, { selector, root }] of [...HIDDEN.entries()]) if (selector.attribute !== void 0) {
			if (root.isConnected === false || element.isConnected === false || cachedQuerySelector(root, selector, cache).has(element) === false) {
				HIDDEN.delete(element);
				element.removeAttribute(selector.attribute);
			}
		}
	}
	/**
	* Queue `elements` to be processed asynchronously in a batch way (for
	* efficiency). This is important to not do more work than necessary, for
	* example if the same set of nodes is updated multiple times in a raw on
	* user-interaction (e.g. a dropdown); this allows to only check these nodes
	* once, and to not block the UI.
	*/
	function delayedUpdateExtended(elements) {
		if (EXTENDED.size === 0) return;
		if (PENDING.has(window.document.documentElement)) return;
		for (const element of elements) {
			if (element === window.document.documentElement) {
				PENDING.clear();
				PENDING.add(element);
				break;
			}
			PENDING.add(element);
		}
		if (UPDATE_EXTENDED_TIMEOUT === null) UPDATE_EXTENDED_TIMEOUT = setTimeout(() => {
			UPDATE_EXTENDED_TIMEOUT = null;
			updateExtended();
		}, 1e3);
	}
	chrome.runtime.onMessage.addListener((msg) => {
		if (msg.action === "evaluateExtendedSelectors") {
			if (msg.extended && msg.extended.length > 0) {
				for (const selector of msg.extended) EXTENDED.set(selector.id || JSON.stringify(selector.ast), selector);
				delayedUpdateExtended([window.document.documentElement]);
			}
		}
	});
	//#endregion
	//#region src/content_scripts/adblocker/index.js
	/**
	* Ghostery Browser Extension
	* https://www.ghostery.com/
	*
	* Copyright 2017-present Ghostery GmbH. All rights reserved.
	*
	* This Source Code Form is subject to the terms of the Mozilla Public
	* License, v. 2.0. If a copy of the MPL was not distributed with this
	* file, You can obtain one at http://mozilla.org/MPL/2.0
	*/
	var DOMContentLoaded = new Promise((resolve) => {
		if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", resolve, {
			once: true,
			passive: true
		});
		else resolve();
	});
	chrome.runtime.sendMessage({
		action: "injectCosmetics",
		bootstrap: true
	}).then((result) => {
		if (result === false) return;
		DOMContentLoaded.then(() => {
			const monitor = new DOMMonitor((update) => {
				if (update.type === "elements") {
					if (update.elements.length !== 0) delayedUpdateExtended(update.elements);
				} else chrome.runtime.sendMessage({
					...update,
					action: "injectCosmetics"
				});
			});
			monitor.queryAll(window);
			monitor.start(window);
		});
	});
	//#endregion
})();
