//#region node_modules/@adguard/agtree/dist/utils/quotes.js
/**
* @file Utility functions for working with quotes
*/
/**
* Set of all possible quote characters supported by the library
*/
var QUOTE_SET = new Set([
	"'",
	"\"",
	"`"
]);
/**
* Possible quote types for scriptlet parameters
*/
var QuoteType = {
	/**
	* No quotes at all
	*/
	None: "none",
	/**
	* Single quotes (`'`)
	*/
	Single: "single",
	/**
	* Double quotes (`"`)
	*/
	Double: "double",
	/**
	* Backtick quotes (`` ` ``)
	*/
	Backtick: "backtick"
};
/**
* Utility functions for working with quotes
*/
var QuoteUtils = class QuoteUtils {
	/**
	* Escape all unescaped occurrences of the character
	*
	* @param string String to escape
	* @param char Character to escape
	* @returns Escaped string
	*/
	static escapeUnescapedOccurrences(string, char) {
		let result = "";
		for (let i = 0; i < string.length; i += 1) {
			if (string[i] === char && (i === 0 || string[i - 1] !== "\\")) result += "\\";
			result += string[i];
		}
		return result;
	}
	/**
	* Unescape all single escaped occurrences of the character
	*
	* @param string String to unescape
	* @param char Character to unescape
	* @returns Unescaped string
	*/
	static unescapeSingleEscapedOccurrences(string, char) {
		let result = "";
		for (let i = 0; i < string.length; i += 1) {
			if (string[i] === char && string[i - 1] === "\\" && (i === 1 || string[i - 2] !== "\\")) result = result.slice(0, -1);
			result += string[i];
		}
		return result;
	}
	/**
	* Get quote type of the string
	*
	* @param string String to check
	* @returns Quote type of the string
	*/
	static getStringQuoteType(string) {
		if (string.length > 1) {
			if (string.startsWith("'") && string.endsWith("'")) return QuoteType.Single;
			if (string.startsWith("\"") && string.endsWith("\"")) return QuoteType.Double;
			if (string.startsWith("`") && string.endsWith("`")) return QuoteType.Backtick;
		}
		return QuoteType.None;
	}
	/**
	* Set quote type of the string
	*
	* @param string String to set quote type of
	* @param quoteType Quote type to set
	* @returns String with the specified quote type
	*/
	static setStringQuoteType(string, quoteType) {
		const actualQuoteType = QuoteUtils.getStringQuoteType(string);
		switch (quoteType) {
			case QuoteType.None:
				if (actualQuoteType === QuoteType.Single) return QuoteUtils.escapeUnescapedOccurrences(string.slice(1, -1), "'");
				if (actualQuoteType === QuoteType.Double) return QuoteUtils.escapeUnescapedOccurrences(string.slice(1, -1), "\"");
				if (actualQuoteType === QuoteType.Backtick) return QuoteUtils.escapeUnescapedOccurrences(string.slice(1, -1), "`");
				return string;
			case QuoteType.Single:
				if (actualQuoteType === QuoteType.None) return "'" + QuoteUtils.escapeUnescapedOccurrences(string, "'") + "'";
				if (actualQuoteType === QuoteType.Double) return "'" + QuoteUtils.escapeUnescapedOccurrences(QuoteUtils.unescapeSingleEscapedOccurrences(string.slice(1, -1), "\""), "'") + "'";
				if (actualQuoteType === QuoteType.Backtick) return "'" + QuoteUtils.escapeUnescapedOccurrences(QuoteUtils.unescapeSingleEscapedOccurrences(string.slice(1, -1), "`"), "'") + "'";
				return string;
			case QuoteType.Double:
				if (actualQuoteType === QuoteType.None) return "\"" + QuoteUtils.escapeUnescapedOccurrences(string, "\"") + "\"";
				if (actualQuoteType !== QuoteType.Double) return "\"" + QuoteUtils.escapeUnescapedOccurrences(QuoteUtils.unescapeSingleEscapedOccurrences(string.slice(1, -1), "'"), "\"") + "\"";
				return string;
			case QuoteType.Backtick:
				if (actualQuoteType === QuoteType.None) return "`" + QuoteUtils.escapeUnescapedOccurrences(string, "`") + "`";
				if (actualQuoteType !== QuoteType.Backtick) return "`" + QuoteUtils.escapeUnescapedOccurrences(QuoteUtils.unescapeSingleEscapedOccurrences(string.slice(1, -1), "'"), "`") + "`";
				return string;
			default: return string;
		}
	}
	/**
	* Removes bounding quotes from a string, if any
	*
	* @param string Input string
	* @returns String without quotes
	*/
	static removeQuotes(string) {
		if (string.length > 1 && (string[0] === "'" || string[0] === "\"" || string[0] === "`") && string[0] === string[string.length - 1]) return string.slice(1, -1);
		return string;
	}
	/**
	* Removes bounding quotes from a string, if any, and unescapes the escaped quotes,
	* like transforming `'abc\'def'` to `abc'def`.
	*
	* @param string Input string
	* @returns String without quotes
	*/
	static removeQuotesAndUnescape(string) {
		switch (QuoteUtils.getStringQuoteType(string)) {
			case QuoteType.Single: return QuoteUtils.unescapeSingleEscapedOccurrences(string.slice(1, -1), "'");
			case QuoteType.Double: return QuoteUtils.unescapeSingleEscapedOccurrences(string.slice(1, -1), "\"");
			case QuoteType.Backtick: return QuoteUtils.unescapeSingleEscapedOccurrences(string.slice(1, -1), "`");
			default: return string;
		}
	}
	/**
	* Wraps given `strings` with `quote` (defaults to single quote `'`)
	* and joins them with `separator` (defaults to comma+space `, `).
	*
	* @param strings Strings to quote and join.
	* @param quoteType Quote to use.
	* @param separator Separator to use.
	*
	* @returns String with joined items.
	*
	* @example
	* ['abc', 'def']: strings[]  ->  "'abc', 'def'": string
	*/
	static quoteAndJoinStrings(strings, quoteType = QuoteType.Single, separator = `, `) {
		return strings.map((s) => QuoteUtils.setStringQuoteType(s, quoteType)).join(separator);
	}
	/**
	* Convert `""` to `\"` within strings inside of attribute selectors,
	* because it is not compatible with the standard CSS syntax.
	*
	* @param selector CSS selector string.
	*
	* @returns Escaped CSS selector.
	*
	* @note In the legacy syntax, `""` is used to escape double quotes, but it cannot be used
	* in the standard CSS syntax, so we use conversion functions to handle this.
	* @note This function is intended to be used on whole attribute selector or whole selector strings.
	*
	* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#tag-content}
	* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#wildcard}
	*
	* @example
	* ```ts
	* QuoteUtils.escapeAttributeDoubleQuotes('[attr="value with "" quotes"]');
	* QuoteUtils.escapeAttributeDoubleQuotes('div[attr="value with "" quotes"] > span');
	* ```
	*/
	static escapeAttributeDoubleQuotes(selector) {
		const nestingBlockPairs = new Map([
			["(", ")"],
			["[", "]"],
			["'", "'"],
			["\"", "\""],
			["`", "`"]
		]);
		const nestingBlockStack = [];
		const buffer = [];
		for (let i = 0; i < selector.length; i += 1) {
			const char = selector[i];
			if (nestingBlockStack.length === 2 && nestingBlockStack[0] === "]" && nestingBlockStack[1] === "\"") {
				if (char === "\"" && selector[i + 1] === "\"") {
					buffer.push("\\");
					buffer.push("\"");
					i += 1;
					continue;
				}
				buffer.push(char);
				continue;
			}
			if (nestingBlockPairs.has(char) && selector[i - 1] !== "\\") {
				nestingBlockStack.push(nestingBlockPairs.get(char));
				buffer.push(char);
				continue;
			}
			if (nestingBlockStack.length > 0 && char === nestingBlockStack[nestingBlockStack.length - 1] && selector[i - 1] !== "\\") {
				nestingBlockStack.pop();
				buffer.push(char);
				continue;
			}
			buffer.push(char);
		}
		return buffer.join("");
	}
	/**
	* Convert escaped double quotes `\"` to `""` within strings inside of attribute selectors,
	* because it is not compatible with the standard CSS syntax.
	*
	* @param selector CSS selector string.
	*
	* @returns Unescaped CSS selector.
	*
	* @note In the legacy syntax, `""` is used to escape double quotes, but it cannot be used
	* in the standard CSS syntax, so we use conversion functions to handle this.
	* @note This function is intended to be used on whole attribute selector or whole selector strings.
	*
	* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#tag-content}
	* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#wildcard}
	*
	* @example
	* ```ts
	* QuoteUtils.unescapeAttributeDoubleQuotes('[attr="value with \\" quotes"]');
	* QuoteUtils.unescapeAttributeDoubleQuotes('div[attr="value with \\" quotes"] > span');
	* ```
	*/
	static unescapeAttributeDoubleQuotes(selector) {
		const nestingBlockPairs = new Map([
			["(", ")"],
			["[", "]"],
			["'", "'"],
			["\"", "\""],
			["`", "`"]
		]);
		const nestingBlockStack = [];
		const buffer = [];
		for (let i = 0; i < selector.length; i += 1) {
			const char = selector[i];
			if (nestingBlockStack.length === 2 && nestingBlockStack[0] === "]" && nestingBlockStack[1] === "\"") {
				if (char === "\\" && selector[i + 1] === "\"") {
					buffer.push("\"");
					buffer.push("\"");
					i += 1;
					continue;
				}
				buffer.push(char);
				continue;
			}
			if (nestingBlockPairs.has(char) && selector[i - 1] !== "\\") {
				nestingBlockStack.push(nestingBlockPairs.get(char));
				buffer.push(char);
				continue;
			}
			if (nestingBlockStack.length > 0 && char === nestingBlockStack[nestingBlockStack.length - 1] && selector[i - 1] !== "\\") {
				nestingBlockStack.pop();
				buffer.push(char);
				continue;
			}
			buffer.push(char);
		}
		return buffer.join("");
	}
};
//#endregion
export { QUOTE_SET, QuoteType, QuoteUtils };
