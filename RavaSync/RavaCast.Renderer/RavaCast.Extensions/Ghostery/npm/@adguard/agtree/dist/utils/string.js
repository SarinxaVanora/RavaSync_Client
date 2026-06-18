/**
* Utility functions for string manipulation.
*/
var StringUtils = class StringUtils {
	/**
	* Finds the first occurrence of a character that:
	* - isn't preceded by an escape character
	*
	* @param pattern - Source pattern
	* @param searchedCharacter - Searched character
	* @param start - Start index
	* @param escapeCharacter - Escape character, \ by default
	* @param end - End index (excluded)
	* @returns Index or -1 if the character not found
	*/
	static findNextUnescapedCharacter(pattern, searchedCharacter, start = 0, escapeCharacter = "\\", end = pattern.length) {
		for (let i = start; i < end; i += 1) if (pattern[i] === searchedCharacter && pattern[i - 1] !== escapeCharacter) return i;
		return -1;
	}
	/**
	* Finds the first occurrence in backward direction of a character that isn't preceded by an escape character.
	*
	* @param pattern - Source pattern
	* @param searchedCharacter - Searched character
	* @param start - Start index
	* @param escapeCharacter - Escape character, \ by default
	* @param end - End index (Included)
	* @returns Index or -1 if the character not found
	*/
	static findNextUnescapedCharacterBackwards(pattern, searchedCharacter, start = pattern.length - 1, escapeCharacter = "\\", end = 0) {
		for (let i = start; i >= end; i -= 1) if (pattern[i] === searchedCharacter && pattern[i - 1] !== escapeCharacter) return i;
		return -1;
	}
	/**
	* Finds the last occurrence of a character that:
	* - isn't preceded by an escape character
	*
	* @param pattern - Source pattern
	* @param searchedCharacter - Searched character
	* @param escapeCharacter - Escape character, \ by default
	* @returns Index or -1 if the character not found
	*/
	static findLastUnescapedCharacter(pattern, searchedCharacter, escapeCharacter = "\\") {
		for (let i = pattern.length - 1; i >= 0; i -= 1) if (pattern[i] === searchedCharacter && pattern[i - 1] !== escapeCharacter) return i;
		return -1;
	}
	/**
	* Finds the next occurrence of a character that:
	* - isn't preceded by an escape character
	* - isn't followed by the specified character
	*
	* @param pattern - Source pattern
	* @param start - Start index
	* @param searchedCharacter - Searched character
	* @param notFollowedBy - Searched character not followed by this character
	* @param escapeCharacter - Escape character, \ by default
	* @returns Index or -1 if the character not found
	*/
	static findNextUnescapedCharacterThatNotFollowedBy(pattern, start, searchedCharacter, notFollowedBy, escapeCharacter = "\\") {
		for (let i = start; i < pattern.length; i += 1) if (pattern[i] === searchedCharacter && pattern[i + 1] !== notFollowedBy && pattern[i - 1] !== escapeCharacter) return i;
		return -1;
	}
	/**
	* Finds the last occurrence of a character that:
	* - isn't preceded by an escape character
	* - isn't followed by the specified character
	*
	* @param pattern - Source pattern
	* @param searchedCharacter - Searched character
	* @param notFollowedBy - Searched character not followed by this character
	* @param escapeCharacter - Escape character, \ by default
	* @returns Index or -1 if the character not found
	*/
	static findLastUnescapedCharacterThatNotFollowedBy(pattern, searchedCharacter, notFollowedBy, escapeCharacter = "\\") {
		for (let i = pattern.length - 1; i >= 0; i -= 1) if (pattern[i] === searchedCharacter && pattern[i + 1] !== notFollowedBy && pattern[i - 1] !== escapeCharacter) return i;
		return -1;
	}
	/**
	* Finds the next occurrence of a character that:
	* - isn't part of any string literal ('literal' or "literal")
	* - isn't part of any RegExp expression (/regexp/)
	*
	* @param pattern - Source pattern
	* @param searchedCharacter - Searched character
	* @param start - Start index
	* @returns Index or -1 if the character not found
	*/
	static findUnescapedNonStringNonRegexChar(pattern, searchedCharacter, start = 0) {
		let open = null;
		for (let i = start; i < pattern.length; i += 1) if ((pattern[i] === "'" || pattern[i] === "\"" || pattern[i] === "/") && pattern[i - 1] !== "\\") {
			if (open === pattern[i]) open = null;
			else if (open === null) open = pattern[i];
		} else if (open === null && pattern[i] === searchedCharacter && pattern[i - 1] !== "\\") return i;
		return -1;
	}
	/**
	* Finds the last occurrence of a character that is:
	* - not part of any string literal ('literal' or "literal")
	* - not part of any RegExp expression (/regexp/)
	* - not preceded by an escape character.
	*
	* Searches backwards from the end of the pattern.
	*
	* @param pattern Source pattern.
	* @param searchedCharacter Searched character.
	* @param escapeCharacter Escape character, `\` by default.
	*
	* @returns Index of the character or -1 if the character not found.
	*/
	static findLastUnescapedNonStringNonRegexChar(pattern, searchedCharacter, escapeCharacter = "\\") {
		let open = null;
		for (let i = pattern.length - 1; i >= 0; i -= 1) if ((pattern[i] === "'" || pattern[i] === "\"" || pattern[i] === "/") && pattern[i - 1] !== escapeCharacter) {
			if (open === pattern[i]) open = null;
			else if (open === null) open = pattern[i];
		} else if (open === null && pattern[i] === searchedCharacter && pattern[i - 1] !== escapeCharacter) return i;
		return -1;
	}
	/**
	* Finds the next occurrence of a character that:
	* - isn't part of any string literal ('literal' or "literal")
	* - isn't preceded by an escape character
	*
	* @param pattern - Source pattern
	* @param searchedCharacter - Searched character
	* @param start - Start index
	* @param escapeCharacter - Escape character, \ by default
	* @returns Index or -1 if the character not found
	*/
	static findNextUnquotedUnescapedCharacter(pattern, searchedCharacter, start = 0, escapeCharacter = "\\") {
		let openQuote = null;
		for (let i = start; i < pattern.length; i += 1) if ((pattern[i] === "'" || pattern[i] === "\"") && pattern[i - 1] !== escapeCharacter) {
			if (!openQuote) openQuote = pattern[i];
			else if (openQuote === pattern[i]) openQuote = null;
		} else if (pattern[i] === searchedCharacter && pattern[i - 1] !== escapeCharacter) {
			if (!openQuote) return i;
		}
		return -1;
	}
	/**
	* Finds the next occurrence of a character that:
	* - isn't "bracketed"
	* - isn't preceded by an escape character
	*
	* @param pattern - Source pattern
	* @param searchedCharacter - Searched character
	* @param start - Start index
	* @param escapeCharacter - Escape character, \ by default
	* @param openBracket - Open bracket, ( by default
	* @param closeBracket - Close bracket, ( by default
	* @throws If the opening and closing brackets are the same
	* @returns Index or -1 if the character not found
	*/
	static findNextNotBracketedUnescapedCharacter(pattern, searchedCharacter, start = 0, escapeCharacter = "\\", openBracket = "(", closeBracket = ")") {
		if (openBracket === closeBracket) throw new Error("Open and close bracket cannot be the same");
		let depth = 0;
		for (let i = start; i < pattern.length; i += 1) if (pattern[i] === openBracket) depth += 1;
		else if (pattern[i] === closeBracket) depth -= 1;
		else if (depth < 1 && pattern[i] === searchedCharacter && pattern[i - 1] !== escapeCharacter) return i;
		return -1;
	}
	/**
	* Splits the source pattern along characters that:
	* - isn't part of any string literal ('literal' or "literal")
	* - isn't preceded by an escape character
	*
	* @param pattern - Source pattern
	* @param delimeterCharacter - Delimeter character
	* @returns Splitted string
	*/
	static splitStringByUnquotedUnescapedCharacter(pattern, delimeterCharacter) {
		const parts = [];
		let delimeterIndex = -1;
		do {
			const prevDelimeterIndex = delimeterIndex;
			delimeterIndex = StringUtils.findNextUnquotedUnescapedCharacter(pattern, delimeterCharacter, delimeterIndex + 1);
			if (delimeterIndex !== -1) parts.push(pattern.substring(prevDelimeterIndex + 1, delimeterIndex));
			else parts.push(pattern.substring(prevDelimeterIndex + 1, pattern.length));
		} while (delimeterIndex !== -1);
		return parts;
	}
	/**
	* Splits the source pattern along characters that:
	* - isn't part of any string literal ('literal' or "literal")
	* - isn't part of any RegExp expression (/regexp/)
	* - isn't preceded by an escape character
	*
	* @param pattern - Source pattern
	* @param delimeterCharacter - Delimeter character
	* @returns Splitted string
	*/
	static splitStringByUnescapedNonStringNonRegexChar(pattern, delimeterCharacter) {
		const parts = [];
		let delimeterIndex = -1;
		do {
			const prevDelimeterIndex = delimeterIndex;
			delimeterIndex = StringUtils.findUnescapedNonStringNonRegexChar(pattern, delimeterCharacter, delimeterIndex + 1);
			if (delimeterIndex !== -1) parts.push(pattern.substring(prevDelimeterIndex + 1, delimeterIndex));
			else parts.push(pattern.substring(prevDelimeterIndex + 1, pattern.length));
		} while (delimeterIndex !== -1);
		return parts;
	}
	/**
	* Splits the source pattern along characters that:
	* - isn't preceded by an escape character
	*
	* @param pattern - Source pattern
	* @param delimeterCharacter - Delimeter character
	* @returns Splitted string
	*/
	static splitStringByUnescapedCharacter(pattern, delimeterCharacter) {
		const parts = [];
		let delimeterIndex = -1;
		do {
			const prevDelimeterIndex = delimeterIndex;
			delimeterIndex = StringUtils.findNextUnescapedCharacter(pattern, delimeterCharacter, delimeterIndex + 1);
			if (delimeterIndex !== -1) parts.push(pattern.substring(prevDelimeterIndex + 1, delimeterIndex));
			else parts.push(pattern.substring(prevDelimeterIndex + 1, pattern.length));
		} while (delimeterIndex !== -1);
		return parts;
	}
	/**
	* Determines whether the given character is a space or tab character.
	*
	* @param char - The character to check.
	* @returns true if the given character is a space or tab character, false otherwise.
	*/
	static isWhitespace(char) {
		return char === " " || char === "	";
	}
	/**
	* Checks if the given character is a digit.
	*
	* @param char The character to check.
	* @returns `true` if the given character is a digit, `false` otherwise.
	*/
	static isDigit(char) {
		return char >= "0" && char <= "9";
	}
	/**
	* Checks if the given character is a small letter.
	*
	* @param char The character to check.
	* @returns `true` if the given character is a small letter, `false` otherwise.
	*/
	static isSmallLetter(char) {
		return char >= "a" && char <= "z";
	}
	/**
	* Checks if the given character is a capital letter.
	*
	* @param char The character to check.
	* @returns `true` if the given character is a capital letter, `false` otherwise.
	*/
	static isCapitalLetter(char) {
		return char >= "A" && char <= "Z";
	}
	/**
	* Checks if the given character is a letter (small or capital).
	*
	* @param char The character to check.
	* @returns `true` if the given character is a letter, `false` otherwise.
	*/
	static isLetter(char) {
		return StringUtils.isSmallLetter(char) || StringUtils.isCapitalLetter(char);
	}
	/**
	* Checks if the given character is a letter or a digit.
	*
	* @param char Character to check
	* @returns `true` if the given character is a letter or a digit, `false` otherwise.
	*/
	static isAlphaNumeric(char) {
		return StringUtils.isLetter(char) || StringUtils.isDigit(char);
	}
	/**
	* Searches for the first non-whitespace character in the source pattern.
	*
	* @param pattern - Source pattern
	* @param start - Start index
	* @returns Index or -1 if the character not found
	*/
	static findFirstNonWhitespaceCharacter(pattern, start = 0) {
		for (let i = start; i < pattern.length; i += 1) if (!StringUtils.isWhitespace(pattern[i])) return i;
		return -1;
	}
	/**
	* Searches for the last non-whitespace character in the source pattern.
	*
	* @param pattern - Source pattern
	* @returns Index or -1 if the character not found
	*/
	static findLastNonWhitespaceCharacter(pattern) {
		for (let i = pattern.length - 1; i >= 0; i -= 1) if (!StringUtils.isWhitespace(pattern[i])) return i;
		return -1;
	}
	/**
	* Finds the next whitespace character in the pattern.
	*
	* @param pattern Pattern to search in
	* @param start Start index
	* @returns Index of the next whitespace character or the length of the pattern if not found
	*/
	static findNextWhitespaceCharacter(pattern, start = 0) {
		for (let i = start; i < pattern.length; i += 1) if (StringUtils.isWhitespace(pattern[i])) return i;
		return pattern.length;
	}
	/**
	* Escapes a specified character in the string.
	*
	* @param pattern - Input string
	* @param character - Character to escape
	* @param escapeCharacter - Escape character (optional)
	* @returns Escaped string
	*/
	static escapeCharacter(pattern, character, escapeCharacter = "\\") {
		let result = "";
		for (let i = 0; i < pattern.length; i += 1) {
			if (pattern[i] === character && pattern[i - 1] !== escapeCharacter) result += escapeCharacter;
			result += pattern[i];
		}
		return result;
	}
	/**
	* Searches for the next non-whitespace character in the source pattern.
	*
	* @param pattern Pattern to search
	* @param start Start index
	* @returns Index of the next non-whitespace character or the length of the pattern
	*/
	static skipWS(pattern, start = 0) {
		let i = start;
		while (i < pattern.length && StringUtils.isWhitespace(pattern[i])) i += 1;
		return Math.min(i, pattern.length);
	}
	/**
	* Searches for the previous non-whitespace character in the source pattern.
	*
	* @param pattern Pattern to search
	* @param start Start index
	* @returns Index of the previous non-whitespace character or -1
	*/
	static skipWSBack(pattern, start = pattern.length - 1) {
		let i = start;
		while (i >= 0 && StringUtils.isWhitespace(pattern[i])) i -= 1;
		return Math.max(i, -1);
	}
	/**
	* Checks if the given character is a new line character.
	*
	* @param char Character to check
	* @returns `true` if the given character is a new line character, `false` otherwise.
	*/
	static isEOL(char) {
		return char === "\r" || char === "\n" || char === "\f";
	}
	/**
	* Splits a string along newline characters.
	*
	* @param input - Input string
	* @returns Splitted string
	*/
	static splitStringByNewLines(input) {
		return input.split(/\r?\n/);
	}
	/**
	* Splits a string by new lines and stores the new line type for each line
	*
	* @param input The input string to be split
	* @returns An array of tuples, where each tuple contains a line of the input string and its
	* corresponding new line type ("lf", "crlf", or "cr")
	*/
	static splitStringByNewLinesEx(input) {
		const result = [];
		let currentLine = "";
		let newLineType = null;
		for (let i = 0; i < input.length; i += 1) {
			const char = input[i];
			if (char === "\r") {
				if (input[i + 1] === "\n") {
					newLineType = "crlf";
					i += 1;
				} else newLineType = "cr";
				result.push([currentLine, newLineType]);
				currentLine = "";
				newLineType = null;
			} else if (char === "\n") {
				newLineType = "lf";
				result.push([currentLine, newLineType]);
				currentLine = "";
				newLineType = null;
			} else currentLine += char;
		}
		if (result.length === 0 || currentLine !== "") result.push([currentLine, newLineType]);
		return result;
	}
	/**
	* Merges an array of tuples (line, newLineType) into a single string
	*
	* @param input The array of tuples to be merged
	* @returns A single string containing the lines and new line characters from the input array
	*/
	static mergeStringByNewLines(input) {
		let result = "";
		for (let i = 0; i < input.length; i += 1) {
			const [line, newLineType] = input[i];
			result += line;
			if (newLineType !== null) if (newLineType === "crlf") result += "\r\n";
			else if (newLineType === "cr") result += "\r";
			else result += "\n";
		}
		return result;
	}
	/**
	* Helper method to parse a raw string as a number
	*
	* @param raw Raw string to parse
	* @returns Parsed number
	* @throws If the raw string can't be parsed as a number
	*/
	static parseNumber(raw) {
		const result = parseInt(raw, 10);
		if (Number.isNaN(result)) throw new Error("Expected a number");
		return result;
	}
	/**
	* Checks if the given value is a string.
	*
	* @param value Value to check
	* @returns `true` if the value is a string, `false` otherwise
	*/
	static isString(value) {
		return typeof value === "string";
	}
	/**
	* Escapes the given characters in the input string.
	*
	* @param input Input string
	* @param characters Characters to escape (by default, no characters are escaped)
	* @returns Escaped string
	*/
	static escapeCharacters(input, characters = /* @__PURE__ */ new Set()) {
		let result = "";
		for (let i = 0; i < input.length; i += 1) {
			if (characters.has(input[i])) result += "\\";
			result += input[i];
		}
		return result;
	}
};
//#endregion
export { StringUtils };
