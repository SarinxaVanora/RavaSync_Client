import { __toESM } from "../../../../../virtual/_rolldown/runtime.js";
import { require_glob_to_regexp } from "../../../../glob-to-regexp/index.js";
//#region node_modules/@adguard/agtree/dist/utils/regexp.js
var import_glob_to_regexp = /* @__PURE__ */ __toESM(require_glob_to_regexp(), 1);
/**
* @file Regular expression utilities
*/
var REGEX_START = "^";
var REGEX_END = "$";
var REGEX_ANY_CHARACTERS = ".*";
var ADBLOCK_URL_START = "||";
var ADBLOCK_URL_START_REGEX = "^(http|https|ws|wss)://([a-z0-9-_.]+\\.)?";
var ADBLOCK_URL_SEPARATOR = "^";
var ADBLOCK_URL_SEPARATOR_REGEX = "([^ a-zA-Z0-9.%_-]|$)";
var ADBLOCK_WILDCARD = "*";
var ADBLOCK_WILDCARD_REGEX = REGEX_ANY_CHARACTERS;
var REGEX_NEGATION_PREFIX = "^((?!";
var REGEX_NEGATION_SUFFIX = ").)*$";
/**
* Special RegExp symbols
*
* @see https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Regular_Expressions#special-escape
*/
var SPECIAL_REGEX_SYMBOLS = new Set([
	"*",
	"^",
	"}",
	")",
	"]",
	"$",
	".",
	"\\",
	"{",
	"(",
	"[",
	"|",
	"+",
	"?",
	"/"
]);
/**
* Utility functions for working with RegExp patterns
*/
var RegExpUtils = class RegExpUtils {
	/**
	* Checks whether a string possibly is a RegExp pattern.
	* Flags are not supported.
	*
	* Note: it does not perform a full validation of the pattern,
	* it just checks if the string starts and ends with a slash.
	*
	* @param pattern - Pattern to check
	* @returns `true` if the string is a RegExp pattern, `false` otherwise
	*/
	static isRegexPattern(pattern) {
		const trimmedPattern = pattern.trim();
		return trimmedPattern.length > 2 && trimmedPattern.startsWith("/") && trimmedPattern.endsWith("/") && trimmedPattern[-1] !== "\\";
	}
	/**
	* Checks whether a string is a negated RegExp pattern.
	*
	* @param pattern - Pattern to check
	* @returns `true` if the string is a negated RegExp pattern, `false` otherwise
	*/
	static isNegatedRegexPattern(pattern) {
		if (pattern.startsWith("/") && pattern.endsWith("/")) {
			const innerPattern = pattern.slice(1, pattern.length - 1);
			return innerPattern.startsWith("^((?!") && innerPattern.endsWith(").)*$");
		}
		return pattern.startsWith("^((?!") && pattern.endsWith(").)*$");
	}
	/**
	* Removes negation from a RegExp pattern.
	*
	* @param pattern - RegExp pattern to remove negation from
	* @returns RegExp pattern without negation
	*/
	static removeNegationFromRegexPattern(pattern) {
		let result = pattern.trim();
		const slashes = RegExpUtils.isRegexPattern(result);
		if (slashes) result = result.substring(1, result.length - 1);
		if (result.startsWith("^((?!") && result.endsWith(").)*$")) result = result.substring(5, result.length - 5);
		return slashes ? `/${result}/` : result;
	}
	/**
	* Negates a RegExp pattern. Technically, this method wraps the pattern in `^((?!` and `).)*$`.
	*
	* RegExp modifiers are not supported.
	*
	* @param pattern Pattern to negate (can be wrapped in slashes or not)
	* @returns Negated RegExp pattern
	*/
	static negateRegexPattern(pattern) {
		let result = pattern.trim();
		let slashes = false;
		if (RegExpUtils.isRegexPattern(result)) {
			result = result.substring(1, result.length - 1);
			slashes = true;
		}
		if (!(result.startsWith("^((?!") && result.endsWith(").)*$"))) {
			if (result.startsWith(REGEX_START)) result = result.substring(REGEX_START.length);
			if (result.endsWith(REGEX_END)) result = result.substring(0, result.length - REGEX_END.length);
			result = `${REGEX_NEGATION_PREFIX}${result}${REGEX_NEGATION_SUFFIX}`;
		}
		if (slashes) result = `/${result}/`;
		return result;
	}
	/**
	* Ensures that a pattern is wrapped in slashes.
	*
	* @param pattern Pattern to ensure slashes for
	* @returns Pattern with slashes
	*/
	static ensureSlashes(pattern) {
		let result = pattern;
		if (!result.startsWith("/")) result = `/${result}`;
		if (!result.endsWith("/")) result += "/";
		return result;
	}
	/**
	* Converts a basic adblock rule pattern to a RegExp pattern. Based on
	* https://github.com/AdguardTeam/tsurlfilter/blob/9b26e0b4a0e30b87690bc60f7cf377d112c3085c/packages/tsurlfilter/src/rules/simple-regex.ts#L219
	*
	* @param pattern Pattern to convert
	* @returns RegExp equivalent of the pattern
	* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#basic-rules}
	*/
	static patternToRegexp(pattern) {
		const trimmed = pattern.trim();
		if (trimmed === ADBLOCK_URL_START || trimmed === "|" || trimmed === ADBLOCK_WILDCARD || trimmed === "") return REGEX_ANY_CHARACTERS;
		if (RegExpUtils.isRegexPattern(pattern)) return pattern.substring(1, pattern.length - 1);
		let result = "";
		let offset = 0;
		let len = trimmed.length;
		if (trimmed[0] === "|") if (trimmed[1] === "|") {
			result += ADBLOCK_URL_START_REGEX;
			offset = ADBLOCK_URL_START.length;
		} else {
			result += REGEX_START;
			offset = REGEX_START.length;
		}
		let trailingPipe = false;
		if (trimmed.endsWith("|")) {
			trailingPipe = true;
			len -= 1;
		}
		for (; offset < len; offset += 1) if (trimmed[offset] === ADBLOCK_WILDCARD) result += ADBLOCK_WILDCARD_REGEX;
		else if (trimmed[offset] === ADBLOCK_URL_SEPARATOR) result += ADBLOCK_URL_SEPARATOR_REGEX;
		else if (SPECIAL_REGEX_SYMBOLS.has(trimmed[offset])) result += "\\" + trimmed[offset];
		else result += trimmed[offset];
		if (trailingPipe) result += REGEX_END;
		return result;
	}
	/**
	* Creates a length-matching regular expression string: /^(?=.{min,max}$).*\/s
	* Where:
	* - (?=.{min,max}$) is a lookahead that ensures the string length is between min and max
	* - .* matches any character (including newlines, due to the 's' flag)
	*
	* @param min Minimum length or `null` for no minimum (default to `0`).
	* @param max Maximum length or `null` for no maximum (default to no maximum).
	*
	* @returns Length-matching regular expression string.
	*/
	static getLengthRegexp(min, max) {
		return `/^(?=.{${min ?? 0},${max ?? ""}}$).*/s`;
	}
	/**
	* Converts a glob pattern to a RegExp string with slashes and 's' flag.
	*
	* @param glob Glob pattern to convert.
	*
	* @returns RegExp string.
	*
	* @example
	* // Returns '/^foo.*bar$/s'
	* RegExpUtils.globToRegExp('foo*bar');
	*/
	static globToRegExp(glob) {
		return `${"/" + (0, import_glob_to_regexp.default)(glob).source + "/"}s`;
	}
};
//#endregion
export { ADBLOCK_URL_SEPARATOR, ADBLOCK_URL_START, RegExpUtils };
