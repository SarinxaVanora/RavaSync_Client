import { EMPTY_UINT32_ARRAY, sizeOfASCII, sizeOfCosmeticSelector, sizeOfRawCosmetic, sizeOfUTF8 } from "../data-view.js";
import { parse } from "../../../../adblocker-extended-selectors/dist/esm/parse.js";
import { SelectorType, classifySelector, destructAST, indexOfPseudoDirective } from "../../../../adblocker-extended-selectors/dist/esm/extended.js";
import "../../../../adblocker-extended-selectors/dist/esm/index.js";
import { HASH_SEED, fastHash, fastHashBetween, getBit, hasUnicode, setBit, tokenize } from "../utils.js";
import { getEntityHashesFromLabelsBackward, getHostnameHashesFromLabelsBackward } from "../request.js";
import { Domains } from "../engine/domains.js";
import { extractHTMLSelectorFromRule } from "../html-filtering.js";
//#region node_modules/@ghostery/adblocker/dist/esm/filters/cosmetic.js
/*!
* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/
var EMPTY_TOKENS = [EMPTY_UINT32_ARRAY];
var DEFAULT_HIDING_STYLE = "display: none !important;";
var REGEXP_UNICODE_COMMA = /* @__PURE__ */ new RegExp(/\\u002C/, "g");
var REGEXP_UNICODE_BACKSLASH = /* @__PURE__ */ new RegExp(/\\u005C/, "g");
var REGEXP_ESCAPED_COMMA = /* @__PURE__ */ new RegExp(/\\,/, "g");
var REGEXP_ESCAPED_SINGLE_QUOTE = /* @__PURE__ */ new RegExp(/\\'/, "g");
var REGEXP_ESCAPED_DOUBLE_QUOTE = /* @__PURE__ */ new RegExp(/\\"/, "g");
var REGEXP_ESCAPED_BACKTICK = /* @__PURE__ */ new RegExp(/\\`/, "g");
/**
* Given a `selector` starting with either '#' or '.' check if what follows is
* a simple CSS selector: /^-?[_a-zA-Z]+[_a-zA-Z0-9-]*$/
*/
function isSimpleSelector(selector) {
	for (let i = 1; i < selector.length; i += 1) {
		const code = selector.charCodeAt(i);
		if (!(code === 45 || code === 95 || code >= 48 && code <= 57 || code >= 65 && code <= 90 || code >= 97 && code <= 122)) {
			if (i < selector.length - 1) {
				const nextCode = selector.charCodeAt(i + 1);
				if (code === 91 || code === 46 || code === 58 || code === 32 && (nextCode === 62 || nextCode === 43 || nextCode === 126 || nextCode === 46 || nextCode === 35)) return true;
			}
			return false;
		}
	}
	return true;
}
/**
* Given a `selector` starting with either 'a[' or '[', check if what follows
* is a simple href attribute selector of the form: 'href^=' or 'href*='.
*/
function isSimpleHrefSelector(selector, start) {
	return selector.startsWith("href^=\"", start) || selector.startsWith("href*=\"", start) || selector.startsWith("href=\"", start);
}
/**
* Validate CSS selector. There is a fast path for simple selectors (e.g.: #foo
* or .bar) which are the most common case. For complex ones, we rely on
* `Element.matches` (if available).
*/
var isValidCss = (() => {
	const div = typeof document !== "undefined" ? document.createElement("div") : { matches: () => {} };
	const matches = (selector) => div.matches(selector);
	const validSelectorRe = /^[#.]?[\w-.]+$/;
	return function isValidCssImpl(selector) {
		if (validSelectorRe.test(selector)) return true;
		try {
			matches(selector);
		} catch (ex) {
			return false;
		}
		return true;
	};
})();
/**
* Masks used to store options of cosmetic filters in a bitmask.
*/
var COSMETICS_MASK;
(function(COSMETICS_MASK) {
	COSMETICS_MASK[COSMETICS_MASK["unhide"] = 1] = "unhide";
	COSMETICS_MASK[COSMETICS_MASK["scriptInject"] = 2] = "scriptInject";
	COSMETICS_MASK[COSMETICS_MASK["isUnicode"] = 4] = "isUnicode";
	COSMETICS_MASK[COSMETICS_MASK["isClassSelector"] = 8] = "isClassSelector";
	COSMETICS_MASK[COSMETICS_MASK["isIdSelector"] = 16] = "isIdSelector";
	COSMETICS_MASK[COSMETICS_MASK["isHrefSelector"] = 32] = "isHrefSelector";
	COSMETICS_MASK[COSMETICS_MASK["extended"] = 64] = "extended";
})(COSMETICS_MASK || (COSMETICS_MASK = {}));
var HASH_DOMAINS_MARKER_LOW = 1;
var HASH_DOMAINS_MARKER_HIGH = 253;
function computeFilterId(mask, selector, domains, parentDomains, style) {
	let hash = HASH_SEED * 37 ^ mask;
	if (selector !== void 0) for (let i = 0; i < selector.length; i += 1) hash = hash * 37 ^ selector.charCodeAt(i);
	if (domains !== void 0) {
		hash = hash * 37 ^ HASH_DOMAINS_MARKER_LOW;
		hash = domains.updateId(hash);
	}
	if (parentDomains !== void 0) {
		hash = hash * 37 ^ HASH_DOMAINS_MARKER_HIGH;
		hash = parentDomains.updateId(hash);
	}
	if (style !== void 0) for (let i = 0; i < style.length; i += 1) hash = hash * 37 ^ style.charCodeAt(i);
	return hash >>> 0;
}
function normalizeSelector(filter, getScriptletCanonicalName) {
	let selector = filter.getSelector();
	if (filter.style !== void 0) selector += filter.style;
	if (filter.isScriptInject() === false) return selector;
	const parsed = filter.parseScript();
	if (parsed === void 0) return selector;
	const canonicalName = getScriptletCanonicalName(parsed.name);
	if (canonicalName === void 0) return selector;
	return selector.replace(parsed.name, canonicalName);
}
/***************************************************************************
*  Cosmetic filters parsing
* ************************************************************************ */
var CosmeticFilter = class CosmeticFilter {
	/**
	* Given a line that we know contains a cosmetic filter, create a CosmeticFiler
	* instance out of it. This function should be *very* efficient, as it will be
	* used to parse tens of thousands of lines.
	*/
	static parse(line, debug = false) {
		const rawLine = line;
		let mask = 0;
		let selector;
		let domains;
		let parentDomains;
		let style;
		const sharpIndex = line.indexOf("#");
		const afterSharpIndex = sharpIndex + 1;
		let suffixStartIndex = afterSharpIndex + 1;
		if (line.length > afterSharpIndex) {
			if (line[afterSharpIndex] === "@") {
				mask = setBit(mask, COSMETICS_MASK.unhide);
				suffixStartIndex += 1;
			} else if (line[afterSharpIndex] === "?") suffixStartIndex += 1;
		}
		if (suffixStartIndex >= line.length) return null;
		if (sharpIndex > 0) {
			const domainEntries = [];
			const parentDomainEntries = [];
			for (const entry of line.slice(0, sharpIndex).split(",")) if (entry.endsWith(">>")) parentDomainEntries.push(entry.slice(0, -2));
			else domainEntries.push(entry);
			if (domainEntries.length !== 0) domains = Domains.parse(domainEntries.join(","), { debug });
			if (parentDomainEntries.length !== 0) parentDomains = Domains.parse(parentDomainEntries.join(","), { debug });
		}
		if (line.length - suffixStartIndex >= 8 && line.endsWith(")") && line.indexOf(":style(", suffixStartIndex) !== -1) {
			const indexOfStyle = line.indexOf(":style(", suffixStartIndex);
			style = line.slice(indexOfStyle + 7, -1);
			line = line.slice(0, indexOfStyle);
		}
		if (line.charCodeAt(suffixStartIndex) === 94) {
			if (line.startsWith("script:has-text(", suffixStartIndex + 1) === false || line.charCodeAt(line.length - 1) !== 41) return null;
			selector = line.slice(suffixStartIndex, line.length);
			if (extractHTMLSelectorFromRule(selector) === void 0) return null;
		} else if (line.length - suffixStartIndex > 4 && line.charCodeAt(suffixStartIndex) === 43 && line.startsWith("+js(", suffixStartIndex)) {
			if ((domains === void 0 || domains.hostnames === void 0 && domains.entities === void 0) && (parentDomains === void 0 || parentDomains.hostnames === void 0 && parentDomains.entities === void 0) && getBit(mask, COSMETICS_MASK.unhide) === false) return null;
			mask = setBit(mask, COSMETICS_MASK.scriptInject);
			selector = line.slice(suffixStartIndex + 4, line.length - 1);
			if (getBit(mask, COSMETICS_MASK.unhide) === false && selector.length === 0) return null;
		} else {
			selector = line.slice(suffixStartIndex);
			const selectorType = classifySelector(selector);
			if (selectorType === SelectorType.Extended) {
				if (selector.slice(0, indexOfPseudoDirective(selector)).trim().length === 0) return null;
				mask = setBit(mask, COSMETICS_MASK.extended);
			} else if (selectorType === SelectorType.Invalid || !isValidCss(selector)) return null;
		}
		if (parentDomains !== void 0 && getBit(mask, COSMETICS_MASK.scriptInject) === false) return null;
		if (domains === void 0 && getBit(mask, COSMETICS_MASK.extended) === true) return null;
		if (selector !== void 0) {
			if (hasUnicode(selector)) mask = setBit(mask, COSMETICS_MASK.isUnicode);
			if (getBit(mask, COSMETICS_MASK.scriptInject) === false && getBit(mask, COSMETICS_MASK.extended) === false && selector.startsWith("^") === false) {
				const c0 = selector.charCodeAt(0);
				const c1 = selector.charCodeAt(1);
				const c2 = selector.charCodeAt(2);
				if (getBit(mask, COSMETICS_MASK.scriptInject) === false) {
					if (c0 === 46 && isSimpleSelector(selector)) mask = setBit(mask, COSMETICS_MASK.isClassSelector);
					else if (c0 === 35 && isSimpleSelector(selector)) mask = setBit(mask, COSMETICS_MASK.isIdSelector);
					else if (c0 === 97 && c1 === 91 && c2 === 104 && isSimpleHrefSelector(selector, 2)) mask = setBit(mask, COSMETICS_MASK.isHrefSelector);
					else if (c0 === 91 && c1 === 104 && isSimpleHrefSelector(selector, 1)) mask = setBit(mask, COSMETICS_MASK.isHrefSelector);
				}
			}
		}
		return new CosmeticFilter({
			mask,
			rawLine: debug === true ? rawLine : void 0,
			selector,
			style,
			domains,
			parentDomains
		});
	}
	/**
	* Deserialize cosmetic filters. The code accessing the buffer should be
	* symetrical to the one in `serializeCosmeticFilter`.
	*/
	static deserialize(buffer) {
		const mask = buffer.getUint16();
		const isUnicode = getBit(mask, COSMETICS_MASK.isUnicode);
		const optionalParts = buffer.getUint8();
		return new CosmeticFilter({
			mask,
			selector: isUnicode ? buffer.getUTF8() : buffer.getCosmeticSelector(),
			domains: (optionalParts & 1) === 1 ? Domains.deserialize(buffer) : void 0,
			rawLine: (optionalParts & 2) === 2 ? buffer.getRawCosmetic() : void 0,
			style: (optionalParts & 4) === 4 ? buffer.getASCII() : void 0,
			parentDomains: (optionalParts & 8) === 8 ? Domains.deserialize(buffer) : void 0
		});
	}
	constructor({ mask, selector, domains, rawLine, style, parentDomains }) {
		this.mask = mask;
		this.selector = selector;
		this.domains = domains;
		this.parentDomains = parentDomains;
		this.style = style;
		this.id = void 0;
		this.rawLine = rawLine;
		this.scriptletDetails = void 0;
	}
	isCosmeticFilter() {
		return true;
	}
	isNetworkFilter() {
		return false;
	}
	/**
	* The format of a cosmetic filter is:
	*
	* | mask | selector length | selector... | hostnames length | hostnames...
	*   32     16                              16
	*
	* The header (mask) is 32 bits, then we have a total of 32 bits to store the
	* length of `selector` and `hostnames` (16 bits each).
	*
	* Improvements similar to the onces mentioned in `serializeNetworkFilters`
	* could be applied here, to get a more compact representation.
	*/
	serialize(buffer) {
		buffer.pushUint16(this.mask);
		const index = buffer.getPos();
		buffer.pushUint8(0);
		if (this.isUnicode()) buffer.pushUTF8(this.selector);
		else buffer.pushCosmeticSelector(this.selector);
		let optionalParts = 0;
		if (this.domains !== void 0) {
			optionalParts |= 1;
			this.domains.serialize(buffer);
		}
		if (this.rawLine !== void 0) {
			optionalParts |= 2;
			buffer.pushRawCosmetic(this.rawLine);
		}
		if (this.style !== void 0) {
			optionalParts |= 4;
			buffer.pushASCII(this.style);
		}
		if (this.parentDomains !== void 0) {
			optionalParts |= 8;
			this.parentDomains.serialize(buffer);
		}
		buffer.setByte(index, optionalParts);
	}
	/**
	* Return an estimation of the size (in bytes) needed to persist this filter
	* in a DataView. This does not need to be 100% accurate but should be an
	* upper-bound. It should also be as fast as possible.
	*/
	getSerializedSize(compression) {
		let estimate = 3;
		if (this.isUnicode()) estimate += sizeOfUTF8(this.selector);
		else estimate += sizeOfCosmeticSelector(this.selector, compression);
		if (this.domains !== void 0) estimate += this.domains.getSerializedSize();
		if (this.parentDomains !== void 0) estimate += this.parentDomains.getSerializedSize();
		if (this.rawLine !== void 0) estimate += sizeOfRawCosmetic(this.rawLine, compression);
		if (this.style !== void 0) estimate += sizeOfASCII(this.style);
		return estimate;
	}
	/**
	* Create a more human-readable version of this filter. It is mainly used for
	* debugging purpose, as it will expand the values stored in the bit mask.
	*/
	toString() {
		if (this.rawLine !== void 0) return this.rawLine;
		let filter = "";
		if (this.domains !== void 0) if (this.domains.parts !== void 0) filter += this.domains.parts;
		else filter += "<hostnames>";
		if (this.parentDomains !== void 0) {
			if (this.domains !== void 0) filter += ",";
			if (this.parentDomains.parts !== void 0) filter += this.parentDomains.parts.split(",").map((part) => part + ">>").join(",");
			else filter += "<hostnames>>>";
		}
		if (this.isUnhide()) filter += "#@#";
		else filter += "##";
		if (this.isScriptInject()) {
			filter += "+js(";
			filter += this.selector;
			filter += ")";
		} else filter += this.selector;
		if (this.hasCustomStyle()) filter += ":style(" + this.getStyle() + ")";
		return filter;
	}
	match(hostname, domain, ancestors) {
		if (this.hasHostnameConstraint() === false) return true;
		if (!hostname) return false;
		if (this.domains !== void 0 && this.domains.match(getHostnameHashesFromLabelsBackward(hostname, domain), getEntityHashesFromLabelsBackward(hostname, domain))) return true;
		if (ancestors !== void 0 && this.parentDomains !== void 0) {
			for (const { hostname, domain } of ancestors) if (this.parentDomains.match(hostname.length === 0 ? EMPTY_UINT32_ARRAY : getHostnameHashesFromLabelsBackward(hostname, domain), hostname.length === 0 ? EMPTY_UINT32_ARRAY : getEntityHashesFromLabelsBackward(hostname, domain))) return true;
		}
		return false;
	}
	/**
	* Get tokens for this filter. It can be indexed multiple times if multiple
	* hostnames are specified (e.g.: host1,host2##.selector).
	*/
	getTokens() {
		const tokens = [];
		if (this.domains !== void 0) {
			const { hostnames, entities } = this.domains;
			if (hostnames !== void 0) for (const hostname of hostnames) tokens.push(new Uint32Array([hostname]));
			if (entities !== void 0) for (const entity of entities) tokens.push(new Uint32Array([entity]));
		}
		if (this.parentDomains !== void 0) {
			const { hostnames, entities } = this.parentDomains;
			if (hostnames !== void 0) for (const hostname of hostnames) tokens.push(new Uint32Array([hostname]));
			if (entities !== void 0) for (const entity of entities) tokens.push(new Uint32Array([entity]));
		}
		if (tokens.length === 0 && this.isUnhide() === false) {
			if (this.isIdSelector() || this.isClassSelector()) {
				let endOfSelector = 1;
				const selector = this.selector;
				for (; endOfSelector < selector.length; endOfSelector += 1) {
					const code = selector.charCodeAt(endOfSelector);
					if (code === 32 || code === 46 || code === 58 || code === 91) break;
				}
				const arr = new Uint32Array(1);
				arr[0] = fastHashBetween(selector, 1, endOfSelector);
				tokens.push(arr);
			} else if (this.isHrefSelector() === true) {
				const selector = this.getSelector();
				let hrefIndex = selector.indexOf("href");
				if (hrefIndex === -1) return EMPTY_TOKENS;
				hrefIndex += 4;
				let skipFirstToken = false;
				let skipLastToken = true;
				if (selector.charCodeAt(hrefIndex) === 42) {
					skipFirstToken = true;
					hrefIndex += 1;
				} else if (selector.charCodeAt(hrefIndex) === 94) hrefIndex += 1;
				else skipLastToken = false;
				hrefIndex += 2;
				const hrefEnd = selector.indexOf("\"", hrefIndex);
				if (hrefEnd === -1) return EMPTY_TOKENS;
				tokens.push(tokenize(this.selector.slice(hrefIndex, hrefEnd), skipFirstToken, skipLastToken));
			}
		}
		if (tokens.length === 0) return EMPTY_TOKENS;
		return tokens;
	}
	parseScript() {
		if (this.scriptletDetails !== void 0) return this.scriptletDetails;
		const selector = this.getSelector();
		if (selector.length === 0) return;
		const parts = [];
		let index = 0;
		let lastComaIndex = -1;
		let inDoubleQuotes = false;
		let inSingleQuotes = false;
		let inBackticks = false;
		let inRegexp = false;
		let objectNesting = 0;
		let lastCharIsBackslash = false;
		let inArgument = false;
		for (; index < selector.length; index += 1) {
			const char = selector[index];
			if (lastCharIsBackslash === false) {
				if (inDoubleQuotes === true) {
					if (char === "\"") inDoubleQuotes = false;
				} else if (inSingleQuotes === true) {
					if (char === "'") inSingleQuotes = false;
				} else if (inBackticks === true) {
					if (char === "`") inBackticks = false;
				} else if (objectNesting !== 0) {
					if (char === "{") objectNesting += 1;
					else if (char === "}") objectNesting -= 1;
					else if (char === "\"") inDoubleQuotes = true;
					else if (char === "'") inSingleQuotes = true;
					else if (char === "`") inBackticks = true;
				} else if (inRegexp === true) {
					if (char === "/") inRegexp = false;
				} else if (inArgument === false) if (char === " ") {} else if (char === "\"" && selector.indexOf("\"", index + 1) > 0) inDoubleQuotes = true;
				else if (char === "'" && selector.indexOf("'", index + 1) > 0) inSingleQuotes = true;
				else if (char === "`" && selector.indexOf("`", index + 1) > 0) inBackticks = true;
				else if (char === "{" && selector.indexOf("}", index + 1) > 0) objectNesting += 1;
				else if (char === "/" && selector.indexOf("/", index + 1) > 0) inRegexp = true;
				else inArgument = true;
			}
			if (lastCharIsBackslash === false && char === "," && inDoubleQuotes === false && inSingleQuotes === false && inBackticks === false && inRegexp === false) {
				parts.push(selector.slice(lastComaIndex + 1, index).trim());
				lastComaIndex = index;
				inArgument = false;
			}
			lastCharIsBackslash = char === "\\" && !lastCharIsBackslash;
		}
		parts.push(selector.slice(lastComaIndex + 1).trim());
		const args = parts.slice(1).map((part) => {
			const openingCode = part.charCodeAt(0);
			if (!(openingCode === 39 && part.endsWith(`'`)) && !(openingCode === 34 && part.endsWith(`"`)) && !(openingCode === 96 && part.endsWith("`"))) return part;
			if (part.charCodeAt(part.length - 2) === 92) return part;
			if (part.length > 2) {
				for (let i = 1; i < part.length - 1; i++) if (part.charCodeAt(i) === openingCode && part.charCodeAt(i - 1) !== 92) return part;
			}
			const escaped = part.substring(1, part.length - 1);
			if (openingCode === 39) return escaped.replace(REGEXP_ESCAPED_SINGLE_QUOTE, "'");
			else if (openingCode === 34) return escaped.replace(REGEXP_ESCAPED_DOUBLE_QUOTE, "\"");
			return escaped.replace(REGEXP_ESCAPED_BACKTICK, "`");
		}).map((part) => {
			const isObjectLiteral = part.startsWith("{");
			let result = part.replace(REGEXP_UNICODE_COMMA, ",").replace(REGEXP_UNICODE_BACKSLASH, "\\");
			if (!isObjectLiteral) result = result.replace(REGEXP_ESCAPED_COMMA, ",");
			return result;
		});
		this.scriptletDetails = {
			name: parts[0],
			args
		};
		return this.scriptletDetails;
	}
	getScript(getScriptlet) {
		const parsed = this.parseScript();
		if (parsed === void 0) return;
		const { name, args } = parsed;
		let script = getScriptlet(name);
		if (script !== void 0) {
			for (let i = 0; i < args.length; i += 1) {
				const arg = args[i].replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
				script = script.replace(`{{${i + 1}}}`, arg);
			}
			return script;
		}
	}
	hasHostnameConstraint() {
		return this.domains !== void 0 || this.parentDomains !== void 0;
	}
	hasSubframeConstraint() {
		return this.parentDomains !== void 0;
	}
	getId() {
		if (this.id === void 0) this.id = computeFilterId(this.mask, this.selector, this.domains, this.parentDomains, this.style);
		return this.id;
	}
	hasCustomStyle() {
		return this.style !== void 0;
	}
	getStyle(defaultStyle = DEFAULT_HIDING_STYLE) {
		return this.style || defaultStyle;
	}
	getStyleAttributeHash() {
		return `s${fastHash(this.getStyle())}`;
	}
	getSelector() {
		return this.selector;
	}
	getSelectorAST() {
		return parse(this.getSelector());
	}
	getASTComponents() {
		const ast = this.getSelectorAST();
		if (ast === void 0) return;
		return destructAST(ast);
	}
	getExtendedSelector() {
		return extractHTMLSelectorFromRule(this.selector);
	}
	isExtended() {
		return getBit(this.mask, COSMETICS_MASK.extended);
	}
	/**
	* @deprecated `...:remove` is migrated to extended selectors' implementation. Use `getASTComponents` instead to get the "directive" specifically.
	*/
	isRemove() {
		const asts = this.getASTComponents();
		if (asts === void 0 || asts.directive === null) return false;
		return asts.directive.name === "remove";
	}
	isUnhide() {
		return getBit(this.mask, COSMETICS_MASK.unhide);
	}
	isScriptInject() {
		return getBit(this.mask, COSMETICS_MASK.scriptInject);
	}
	isCSS() {
		return this.isScriptInject() === false;
	}
	isIdSelector() {
		return getBit(this.mask, COSMETICS_MASK.isIdSelector);
	}
	isClassSelector() {
		return getBit(this.mask, COSMETICS_MASK.isClassSelector);
	}
	isHrefSelector() {
		return getBit(this.mask, COSMETICS_MASK.isHrefSelector);
	}
	isUnicode() {
		return getBit(this.mask, COSMETICS_MASK.isUnicode);
	}
	isHtmlFiltering() {
		return this.getSelector().startsWith("^");
	}
	isGenericHide() {
		return this?.domains?.hostnames === void 0 && this?.domains?.entities === void 0 && this?.parentDomains?.hostnames === void 0 && this?.parentDomains?.entities === void 0;
	}
};
//#endregion
export { DEFAULT_HIDING_STYLE, CosmeticFilter as default, normalizeSelector };
