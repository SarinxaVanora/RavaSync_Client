//#region node_modules/@adguard/css-tokenizer/dist/csstokenizer.mjs
/**
* @file Implementation of CSS Syntax Module Level 3 tokenizer definitions (§ 4.2.)
*
* @see {@link https://www.w3.org/TR/css-syntax-3/#tokenizer-definitions}
*/
/**
* Check if code point code is between two code points
*
* @param code Code point to check
* @param min Minimum code point
* @param max Maximum code point
* @returns `true` if code point is between `min` and `max`, `false` otherwise
* @note Boundaries are inclusive
* @note This function is used instead of `code >= min && code <= max` because TypeScript doesn't allow to compare
* `number | undefined` with `number` (even though it's perfectly valid in JavaScript)
*/
function isBetween(code, min, max) {
	return code >= min && code <= max;
}
/**
* Check if code point code is greater than or equal to other code point
*
* @param code Code point to check
* @param min Minimum code point
* @returns `true` if code point is greater than or equal to `min`, `false` otherwise
* @note This function is used instead of `code >= min` because TypeScript doesn't allow to compare
* `number | undefined` with `number` (even though it's perfectly valid in JavaScript)
*/
function isGreaterThanOrEqual(code, min) {
	return code >= min;
}
/**
* Check if character code is a digit
*
* @param code Character code
* @returns `true` if character code is a digit, `false` otherwise
* @see {@link https://www.w3.org/TR/css-syntax-3/#digit}
*/
function isDigit(code) {
	return isBetween(code, 48, 57);
}
/**
* Check if character code is a hex digit
*
* @param code Character code
* @returns `true` if character code is a hex digit, `false` otherwise
* @see {@link https://www.w3.org/TR/css-syntax-3/#hex-digit}
*/
function isHexDigit(code) {
	return isDigit(code) || isBetween(code, 65, 70) || isBetween(code, 97, 102);
}
/**
* Check if character code is an uppercase letter
*
* @param code Character code
* @returns `true` if character code is an uppercase letter, `false` otherwise
* @see {@link https://www.w3.org/TR/css-syntax-3/#uppercase-letter}
*/
function isUppercaseLetter(code) {
	return isBetween(code, 65, 90);
}
/**
* Check if character code is a lowercase letter
*
* @param code Character code
* @returns `true` if character code is a lowercase letter, `false` otherwise
* @see {@link https://www.w3.org/TR/css-syntax-3/#lowercase-letter}
*/
function isLowercaseLetter(code) {
	return isBetween(code, 97, 122);
}
/**
* Check if character code is a letter
*
* @param code Character code
* @returns `true` if character code is a letter, `false` otherwise
* @see {@link https://www.w3.org/TR/css-syntax-3/#letter}
*/
function isLetter(code) {
	return isUppercaseLetter(code) || isLowercaseLetter(code);
}
/**
* Check if character code is a non-ASCII code point
*
* @param code Character code
* @returns `true` if character code is a non-ASCII code point, `false` otherwise
* @see {@link https://www.w3.org/TR/css-syntax-3/#non-ascii-code-point}
*/
function isNonAsciiCodePoint(code) {
	return isGreaterThanOrEqual(code, 128);
}
/**
* Check if character code is a name code point
*
* @param code Character code
* @returns `true` if character code is a name start code point, `false` otherwise
* @see {@link https://www.w3.org/TR/css-syntax-3/#ident-start-code-point}
*/
function isIdentStartCodePoint(code) {
	return isLetter(code) || isNonAsciiCodePoint(code) || code === 95;
}
/**
* Check if character code is a name code point
*
* @param code Character code
* @returns `true` if character code is a name code point, `false` otherwise
* @see {@link https://www.w3.org/TR/css-syntax-3/#ident-code-point}
*/
function isIdentCodePoint(code) {
	return isIdentStartCodePoint(code) || isDigit(code) || code === 45;
}
/**
* Check if character code is a non-printable code point
*
* @param code Character code
* @returns `true` if character code is a non-printable code point, `false` otherwise
* @see {@link https://www.w3.org/TR/css-syntax-3/#non-printable-code-point}
*/
function isNonPrintableCodePoint(code) {
	return isBetween(code, 0, 8) || code === 11 || isBetween(code, 14, 31) || code === 127;
}
/**
* Check if character code is a newline
*
* @param code Character code
* @returns `true` if character code is a newline, `false` otherwise
* @see {@link https://www.w3.org/TR/css-syntax-3/#newline}
*/
function isNewline(code) {
	return code === 10 || code === 13 || code === 12;
}
/**
* Check if character code is a whitespace
*
* @param code Character code
* @returns `true` if character code is a whitespace, `false` otherwise
* @see {@link https://www.w3.org/TR/css-syntax-3/#whitespace}
*/
function isWhitespace(code) {
	return isNewline(code) || code === 9 || code === 32;
}
/**
* Check if character code is a BOM (Byte Order Mark)
*
* @param code Character code to check
* @returns `true` if character code is a BOM, `false` otherwise
*/
function isBOM(code) {
	return code === 65279 || code === 65534;
}
/**
* § 4.3.8. Check if two code points are a valid escape
*
* @param a First code point
* @param b Second code point
* @returns `true` if the code points are a valid escape, `false` otherwise
* @see {@link https://www.w3.org/TR/css-syntax-3/#starts-with-a-valid-escape}
* @note This algorithm will not consume any additional code point.
*/
var checkForValidEscape = (a, b) => {
	if (a !== 92) return false;
	return !isNewline(b);
};
/**
* § 4.3.9. Check if three code points would start an ident sequence
*
* @param a First code point
* @param b Second code point
* @param c Third code point
* @returns `true` if the next code points would start an identifier, `false` otherwise
* @see {@link https://www.w3.org/TR/css-syntax-3/#would-start-an-identifier}
* @note This algorithm will not consume any additional code points.
*/
var checkForIdentStart = (a, b, c) => {
	if (a === 45) return isIdentStartCodePoint(b) || b === 45 || checkForValidEscape(b, c);
	if (isIdentStartCodePoint(a)) return true;
	if (a === 92) return checkForValidEscape(a, b);
	return false;
};
/**
* § 4.3.10. Check if three code points would start a number
*
* @param a First code point
* @param b Second code point
* @param c Third code point
* @returns `true` if the next code points would start a number, `false` otherwise
* @see {@link https://www.w3.org/TR/css-syntax-3/#starts-with-a-number}
* @note This algorithm will not consume any additional code points.
*/
var checkForNumberStart = (a, b, c) => {
	if (a === 43 || a === 45) {
		if (isDigit(b)) return true;
		return b === 46 && isDigit(c);
	}
	if (a === 46) return isDigit(b);
	return isDigit(a);
};
/**
* @file Hashing functions based on the djb2 algorithm
*
* @see {@link http://www.cse.yorku.ca/~oz/hash.html}
* @see {@link https://gist.github.com/eplawless/52813b1d8ad9af510d85?permalink_comment_id=3367765#gistcomment-3367765}
* @todo If we need it, we can create case-sensitive versions of these functions
*/
/**
* Make a unique hash from the given array of code points
*
* @param arr Reference to the array of code points
* @param start Start index
* @param end End index
* @returns Hash of the given array of code points
* @note Case-insensitive (we use it just for function names which are case-insensitive)
*/
function getCodePointsArrayHash(arr, start, end) {
	let hash = 5381;
	for (let i = start; i < end; i += 1) hash = hash * 33 ^ (arr[i] | 32);
	return hash >>> 0;
}
/**
* @file Tokenizer context
*/
/**
* Context of the tokenizer which is shared between all the functions
*/
var TokenizerContext = class {
	/**
	* Cached source length
	*/
	length;
	/**
	* Reference to the `onToken` callback function
	*/
	onToken;
	/**
	* Reference to the `onError` callback function
	*/
	onError;
	/**
	* Unicode code points of the source string
	*
	* @note The last code point is always EOF ("imaginary" code point)
	* @note Using `!` is safe here because the `preprocess` function always sets the codes in the constructor
	* @note We need a signed 32-bit integer array, because the code points are 21-bit integers + imaginary code points
	* are negative numbers
	*/
	codes;
	/**
	* Actual position in the source string
	*/
	cursor;
	/**
	* Custom function handlers to handle special functions, like Extended CSS's pseudo selectors
	*/
	customFunctionHandlers;
	/**
	* Constructs a new tokenizer context instance
	*
	* @param source Source string
	* @param onToken Callback function to call when a token is found
	* @param onError Callback function to call when a parsing error occurs
	* @param functionHandlers Custom function handlers to handle special functions, like Extended CSS's pseudo
	* selectors
	*/
	constructor(source, onToken, onError, functionHandlers) {
		this.length = source.length;
		this.preprocess(source);
		this.cursor = isBOM(this.codes[0]) ? 1 : 0;
		this.onToken = onToken;
		this.onError = onError;
		if (functionHandlers) {
			this.customFunctionHandlers = /* @__PURE__ */ new Map();
			for (const [hash, handler] of functionHandlers) this.customFunctionHandlers.set(hash, handler);
		}
	}
	/**
	* § 3.3. Preprocessing the input stream
	*
	* @param source Source string to preprocess
	* @see {@link https://www.w3.org/TR/css-syntax-3/#input-preprocessing}
	*/
	preprocess(source) {
		const len = source.length;
		this.codes = new Int32Array(len + 1);
		for (let i = 0; i < len; i += 1) this.codes[i] = source.charCodeAt(i);
		this.codes[len] = -1;
	}
	/**
	* Gets the corresponding custom function handler for the given function name hash
	*
	* @param hash Function name hash
	* @returns Corresponding custom function handler or `undefined` if not found
	*/
	getFunctionHandler(hash) {
		return this.customFunctionHandlers?.get(hash);
	}
	/**
	* Checks if the custom function handler is registered for the given function name hash
	*
	* @param hash Custom function name hash
	* @returns `true` if the custom function handler is registered, `false` otherwise
	*/
	hasFunctionHandler(hash) {
		return this.customFunctionHandlers?.has(hash) ?? false;
	}
	/**
	* Returns the current offset
	*
	* @returns Current offset
	*/
	get offset() {
		return this.cursor;
	}
	/**
	* Returns the code point at the current offset
	*
	* @returns Code point at the current offset
	*/
	get code() {
		return this.codes[this.offset];
	}
	/**
	* Returns the code point at the previous offset
	*
	* @returns Code point at the previous offset or `undefined` if the offset is out of bounds
	*/
	get prevCode() {
		return this.codes[this.offset - 1];
	}
	/**
	* Returns the code point at the next offset
	*
	* @returns Code point at the next offset or `undefined` if the offset is out of bounds
	*/
	get nextCode() {
		return this.codes[this.offset + 1];
	}
	/**
	* Returns the code point at the given relative offset
	*
	* @param relativeOffset Relative offset
	* @returns Code point at the relative offset or `undefined` if the offset is out of bounds
	* @note Relative offset compared to the current offset. 1 means the next code point, -1 means the previous code
	* point, 2 means the code point after the next code point, etc.
	*/
	getRelativeCode(relativeOffset) {
		return this.codes[this.offset + relativeOffset];
	}
	/**
	* Check if the current offset is at the end of the source (or past it)
	*
	* @returns `true` if the current offset is at the end of the source, `false` otherwise
	*/
	isEof() {
		return this.offset >= this.length;
	}
	/**
	* Stops the tokenizer by moving the cursor to the end of the input.
	*
	* @note This method is defined as an arrow function to ensure it retains the correct `this` context.
	* Since `stop` is always passed to the `onToken` callback, which is invoked frequently during tokenization,
	* avoiding unnecessary overhead is crucial.
	* Using an arrow function provides better performance compared to binding the method in the constructor
	* or at the call site.
	*/
	stop = () => {
		this.cursor = this.length;
	};
	/**
	* Check if the next code point is EOF
	*
	* @returns `true` if the next code point is EOF, `false` otherwise
	*/
	isNextEof() {
		return this.cursor + 1 === this.length;
	}
	/**
	* Check if the current offset is less than or equal to the end of the source
	*
	* @returns `true` if the current offset is less than or equal to the end of the source, `false` otherwise
	*/
	isLessThanEqualToEof() {
		return this.offset <= this.length;
	}
	/**
	* Consumes the given number of code points
	*
	* @param n Number of code points to consume (default: 1)
	* @note Negative numbers are allowed (they will move the cursor backwards)
	* @note No protection against out of bounds for performance reasons
	*/
	consumeCodePoint(n = 1) {
		this.cursor += n;
	}
	/**
	* Finds the next non-whitespace code point and returns it
	*
	* @returns Next non-whitespace code point or EOF imaginary code point if the rest of the source is whitespace
	*/
	getNextNonWsCode() {
		let i = this.cursor;
		while (i < this.length && isWhitespace(this.codes[i])) i += 1;
		return this.codes[i];
	}
	/**
	* Consumes the whitespace code points
	*/
	consumeWhitespace() {
		while (this.code && isWhitespace(this.code)) this.consumeCodePoint();
	}
	/**
	* Consumes a single whitespace code point, if the current code point is a whitespace
	*/
	consumeSingleWhitespace() {
		if (isWhitespace(this.code)) this.cursor += this.code === 13 && this.nextCode === 10 ? 2 : 1;
	}
	/**
	* Consumes everything until the end of the comment (or the end of the source)
	*/
	consumeUntilCommentEnd() {
		while (this.cursor < this.length) {
			if (this.code === 42 && this.nextCode === 47) {
				this.cursor += 2;
				break;
			}
			this.cursor += 1;
		}
	}
	/**
	* Consumes a single-character token (trivial token) and reports it via the `onToken` callback
	*
	* @param tokenType Token type to report
	*/
	consumeTrivialToken(tokenType) {
		this.onToken(tokenType, this.cursor, ++this.cursor, void 0, this.stop);
	}
	/**
	* Calculates the hash of the fragment from the given start offset to the current offset. This is useful to
	* fast-check function names.
	*
	* @param start Start offset
	* @returns Calculated hash
	*/
	getHashFrom(start) {
		return getCodePointsArrayHash(this.codes, start, this.cursor);
	}
};
/**
* @file Possible CSS token types, as defined in the CSS Syntax Module Level 3.
*
* ! Strictly follows the spec.
*
* @see {@link https://www.w3.org/TR/css-syntax-3/#tokenization}
*/
var TokenType;
(function(TokenType) {
	TokenType[TokenType["Eof"] = 0] = "Eof";
	TokenType[TokenType["Ident"] = 1] = "Ident";
	TokenType[TokenType["Function"] = 2] = "Function";
	TokenType[TokenType["AtKeyword"] = 3] = "AtKeyword";
	TokenType[TokenType["Hash"] = 4] = "Hash";
	TokenType[TokenType["String"] = 5] = "String";
	TokenType[TokenType["BadString"] = 6] = "BadString";
	TokenType[TokenType["Url"] = 7] = "Url";
	TokenType[TokenType["BadUrl"] = 8] = "BadUrl";
	TokenType[TokenType["Delim"] = 9] = "Delim";
	TokenType[TokenType["Number"] = 10] = "Number";
	TokenType[TokenType["Percentage"] = 11] = "Percentage";
	TokenType[TokenType["Dimension"] = 12] = "Dimension";
	TokenType[TokenType["Whitespace"] = 13] = "Whitespace";
	TokenType[TokenType["Cdo"] = 14] = "Cdo";
	TokenType[TokenType["Cdc"] = 15] = "Cdc";
	TokenType[TokenType["Colon"] = 16] = "Colon";
	TokenType[TokenType["Semicolon"] = 17] = "Semicolon";
	TokenType[TokenType["Comma"] = 18] = "Comma";
	TokenType[TokenType["OpenSquareBracket"] = 19] = "OpenSquareBracket";
	TokenType[TokenType["CloseSquareBracket"] = 20] = "CloseSquareBracket";
	TokenType[TokenType["OpenParenthesis"] = 21] = "OpenParenthesis";
	TokenType[TokenType["CloseParenthesis"] = 22] = "CloseParenthesis";
	TokenType[TokenType["OpenCurlyBracket"] = 23] = "OpenCurlyBracket";
	TokenType[TokenType["CloseCurlyBracket"] = 24] = "CloseCurlyBracket";
	TokenType[TokenType["Comment"] = 25] = "Comment";
})(TokenType || (TokenType = {}));
/**
* @file CSS token names
*/
var UNKNOWN_TOKEN_NAME = "unknown";
/**
* Pairs of token types and their base names
*/
var TOKEN_NAMES = Object.freeze({
	[TokenType.Eof]: "eof",
	[TokenType.Ident]: "ident",
	[TokenType.Function]: "function",
	[TokenType.AtKeyword]: "at-keyword",
	[TokenType.Hash]: "hash",
	[TokenType.String]: "string",
	[TokenType.BadString]: "bad-string",
	[TokenType.Url]: "url",
	[TokenType.BadUrl]: "bad-url",
	[TokenType.Delim]: "delim",
	[TokenType.Number]: "number",
	[TokenType.Percentage]: "percentage",
	[TokenType.Dimension]: "dimension",
	[TokenType.Whitespace]: "whitespace",
	[TokenType.Cdo]: "CDO",
	[TokenType.Cdc]: "CDC",
	[TokenType.Colon]: "colon",
	[TokenType.Semicolon]: "semicolon",
	[TokenType.Comma]: "comma",
	[TokenType.OpenSquareBracket]: "[",
	[TokenType.CloseSquareBracket]: "]",
	[TokenType.OpenParenthesis]: "(",
	[TokenType.CloseParenthesis]: ")",
	[TokenType.OpenCurlyBracket]: "{",
	[TokenType.CloseCurlyBracket]: "}",
	[TokenType.Comment]: "comment"
});
/**
* Get base token name by token type
*
* @param type Token type
*
* @example
* ```ts
* getBaseTokenName(TokenType.Ident); // 'ident'
* getBaseTokenName(-1); // 'unknown'
* ```
*
* @returns Base token name or 'unknown' if token type is unknown
*/
var getBaseTokenName = (type) => {
	return TOKEN_NAMES[type] ?? UNKNOWN_TOKEN_NAME;
};
/**
* Get formatted token name by token type
*
* @param type Token type
*
* @example
* ```ts
* getFormattedTokenName(TokenType.Ident); // '<ident-token>'
* getFormattedTokenName(-1); // '<unknown-token>'
* ```
*
* @returns Formatted token name or `'<unknown-token>'` if token type is unknown
*/
var getFormattedTokenName = (type) => {
	return `<${getBaseTokenName(type)}-token>`;
};
/**
* @file Tokenizing logic for escaped code points
*/
var MAX_HEX_DIGITS = 6;
/**
* § 4.3.7. Consume an escaped code point
*
* @param context Reference to the tokenizer context instance
* @see {@link https://www.w3.org/TR/css-syntax-3/#consume-escaped-code-point}
*/
var consumeEscapedCodePoint = (context) => {
	context.consumeCodePoint();
	if (isHexDigit(context.code)) {
		let consumedHexDigits = 0;
		while (isHexDigit(context.code) && consumedHexDigits <= MAX_HEX_DIGITS) {
			context.consumeCodePoint();
			consumedHexDigits += 1;
		}
		context.consumeSingleWhitespace();
	}
	if (context.isEof()) context.onError("Unexpected end of file while parsing escaped code point.", context.offset, context.offset);
};
/**
* @file Tokenizing logic for ident sequences
*/
/**
* § 4.3.11. Consume an ident sequence
*
* Consume an ident sequence from a stream of code points. It returns a string containing the largest name that can be
* formed from adjacent code points in the stream, starting from the first.
*
* @param context Reference to the tokenizer context instance
* @see {@link https://www.w3.org/TR/css-syntax-3/#consume-name}
* @note This algorithm does not do the verification of the first few code points that are necessary to ensure the
* returned code points would constitute an <ident-token>. If that is the intended use, ensure that the stream
* starts with an ident sequence before calling this algorithm.
*/
var consumeIndentSequence = (context) => {
	while (!context.isEof()) {
		if (isIdentCodePoint(context.code)) {
			context.consumeCodePoint();
			continue;
		}
		if (checkForValidEscape(context.code, context.nextCode)) {
			context.consumeCodePoint();
			consumeEscapedCodePoint(context);
			continue;
		}
		return;
	}
};
/**
* @file Tokenizing logic for URLs
*/
/**
* § 4.3.14. Consume the remnants of a bad url
*
* Consume the remnants of a bad url from a stream of code points, "cleaning up" after the tokenizer realizes that it’s
* in the middle of a <bad-url-token> rather than a <url-token>. It returns nothing; its sole use is to consume enough
* of the input stream to reach a recovery point where normal tokenizing can resume.
*
* @param context Tokenizer context
* @see {@link https://www.w3.org/TR/css-syntax-3/#consume-remnants-of-bad-url}
*/
function consumeBadUrlRemnants(context) {
	for (; !context.isEof(); context.consumeCodePoint()) {
		if (context.code === 41) {
			context.consumeCodePoint();
			return;
		}
		if (checkForValidEscape(context.getRelativeCode(1), context.getRelativeCode(2))) {
			context.consumeCodePoint();
			consumeEscapedCodePoint(context);
			continue;
		}
	}
}
/**
* Helper function for consuming a bad url token.
*
* @param context Tokenizer context
* @param start Token start offset
* @see {@link https://www.w3.org/TR/css-syntax-3/#consume-remnants-of-bad-url}
*/
function consumeBadUrlToken(context, start) {
	consumeBadUrlRemnants(context);
	context.onToken(TokenType.BadUrl, start, context.offset, void 0, context.stop);
}
/**
* § 4.3.6. Consume a url token
*
* Consume a url token from a stream of code points. It returns either a <url-token> or a <bad-url-token>.
*
* @param context Reference to the tokenizer context instance
* @param start Token start offset
* @see {@link https://www.w3.org/TR/css-syntax-3/#consume-url-token}
* @note This algorithm assumes that the initial "url(" has already been consumed. This algorithm also assumes that
* it’s being called to consume an "unquoted" value, like url(foo). A quoted value, like url("foo"), is parsed as a
* <function-token>. Consume an ident-like token automatically handles this distinction; this algorithm shouldn’t be
* called directly otherwise.
*/
var consumeUrlToken = (context, start) => {
	while (isWhitespace(context.code)) context.consumeCodePoint();
	while (context.offset <= context.length) {
		if (context.code === 41) {
			context.consumeCodePoint();
			context.onToken(TokenType.Url, start, context.offset, void 0, context.stop);
			return;
		}
		if (context.isEof()) {
			context.onToken(TokenType.Url, start, context.offset, void 0, context.stop);
			context.onError("Unexpected end of file while parsing URL.", start, context.offset);
			return;
		}
		if (isWhitespace(context.code)) {
			while (isWhitespace(context.code)) context.consumeCodePoint();
			if (context.code === 41 || context.isEof()) {
				context.consumeCodePoint();
				context.onToken(TokenType.Url, start, context.offset, void 0, context.stop);
				context.onError("Unexpected end of file while parsing URL.", start, context.offset);
				return;
			}
			context.onError("Unexpected character in URL.", start, context.offset);
			consumeBadUrlToken(context, start);
			return;
		}
		if (context.code === 34 || context.code === 39 || context.code === 40 || isNonPrintableCodePoint(context.code)) {
			context.onError("Unexpected character in URL.", start, context.offset);
			consumeBadUrlToken(context, start);
			return;
		}
		if (context.code === 92) {
			if (checkForValidEscape(context.code, context.nextCode)) {
				context.consumeCodePoint();
				consumeEscapedCodePoint(context);
				continue;
			}
			context.onError("Unexpected character in URL.", start, context.offset);
			consumeBadUrlToken(context, start);
			return;
		}
		context.consumeCodePoint();
	}
};
/**
* @file Tokenizing logic for ident-like tokens
*/
var URL_FUNCTION_HASH = 193422222;
/**
* § 4.3.4. Consume an ident-like token
*
* Consume an ident-like token from a stream of code points. It returns an <ident-token>, <function-token>, <url-token>,
* or <bad-url-token>.
*
* @param context Reference to the tokenizer context instance
* @see {@link https://www.w3.org/TR/css-syntax-3/#consume-an-ident-like-token}
* @note We extended the algorithm to allow custom function handlers, but the tokenizer still strictly follows the spec.
*/
var consumeIdentLikeToken = (context) => {
	const start = context.offset;
	consumeIndentSequence(context);
	if (context.code === 40) {
		const fnHash = context.getHashFrom(start);
		context.consumeCodePoint();
		if (fnHash === URL_FUNCTION_HASH) {
			const nextNonWsCode = context.getNextNonWsCode();
			if (nextNonWsCode === 34 || nextNonWsCode === 39) {
				context.onToken(TokenType.Function, start, context.offset, void 0, context.stop);
				return;
			}
			consumeUrlToken(context, start);
			return;
		}
		if (context.hasFunctionHandler(fnHash)) {
			context.onToken(TokenType.Function, start, context.offset, void 0, context.stop);
			context.getFunctionHandler(fnHash)(context);
			return;
		}
		context.onToken(TokenType.Function, start, context.offset, void 0, context.stop);
		return;
	}
	context.onToken(TokenType.Ident, start, context.offset, void 0, context.stop);
};
/**
* @file Tokenizing logic for numbers
*/
/**
* § 4.3.12. Consume a number
*
* Consume a number from a stream of code points. It returns a numeric value, and a type which is either "integer" or
* "number".
*
* @param context Reference to the tokenizer context instance
* @note This algorithm does not do the verification of the first few code points that are necessary to ensure a number
* can be obtained from the stream. Ensure that the stream starts with a number before calling this algorithm.
* @see {@link https://www.w3.org/TR/css-syntax-3/#consume-number}
* @todo Uncomment type/repr handling if needed - currently we don't need them, and they're not used for performance
* reasons
*/
var consumeNumber = (context) => {
	if (context.code === 43 || context.code === 45) context.consumeCodePoint();
	while (isDigit(context.code)) context.consumeCodePoint();
	if (context.code === 46 && isDigit(context.nextCode)) {
		context.consumeCodePoint(2);
		while (isDigit(context.code)) context.consumeCodePoint();
	}
	if (context.code === 69 || context.code === 101) {
		if ((context.nextCode === 45 || context.nextCode === 43) && isDigit(context.getRelativeCode(2))) {
			context.consumeCodePoint(3);
			while (isDigit(context.code)) context.consumeCodePoint();
		} else if (isDigit(context.nextCode)) {
			context.consumeCodePoint(2);
			while (isDigit(context.code)) context.consumeCodePoint();
		}
	}
};
/**
* @file Tokenizing logic for numeric tokens
*/
/**
* § 4.3.3. Consume a numeric token
*
* Consume a numeric token from a stream of code points. It returns either a <number-token>, <percentage-token>, or
* <dimension-token>.
*
* @param context Reference to the tokenizer context instance
* @see {@link https://www.w3.org/TR/css-syntax-3/#consume-numeric-token}
*/
var consumeNumericToken = (context) => {
	const start = context.offset;
	consumeNumber(context);
	if (checkForIdentStart(context.code, context.nextCode, context.getRelativeCode(2))) {
		consumeIndentSequence(context);
		context.onToken(TokenType.Dimension, start, context.offset, void 0, context.stop);
		return;
	}
	if (context.code === 37) {
		context.consumeCodePoint();
		context.onToken(TokenType.Percentage, start, context.offset, void 0, context.stop);
		return;
	}
	context.onToken(TokenType.Number, start, context.offset, void 0, context.stop);
};
/**
* @file Tokenizing logic for strings
*/
/**
* § 4.3.5. Consume a string token
*
* Consume a string token from a stream of code points. It returns either a <string-token> or <bad-string-token>.
*
* @param context Reference to the tokenizer context instance
* @see {@link https://www.w3.org/TR/css-syntax-3/#consume-string-token}
*/
var consumeStringToken = (context) => {
	const endingCodePoint = context.code;
	const start = context.offset;
	context.consumeCodePoint();
	while (context.isLessThanEqualToEof()) switch (context.code) {
		case endingCodePoint:
			context.consumeCodePoint();
			context.onToken(TokenType.String, start, context.offset, void 0, context.stop);
			return;
		case -1:
			context.onToken(TokenType.String, start, context.offset, void 0, context.stop);
			context.onError("Unexpected end of file while parsing string token.", start, context.offset);
			return;
		case 13:
		case 10:
		case 12:
			if (context.code === 13 && context.nextCode === 10) context.consumeCodePoint(1);
			context.consumeCodePoint(1);
			context.onToken(TokenType.BadString, start, context.offset, void 0, context.stop);
			context.onError("Unexpected newline while parsing string token.", start, context.offset);
			return;
		case 92:
			if (context.isNextEof()) {
				context.consumeCodePoint();
				context.onToken(TokenType.String, start, context.offset, void 0, context.stop);
				context.onError("Unexpected end of file while parsing string token.", start, context.offset);
				return;
			}
			if (isNewline(context.nextCode)) {
				context.consumeCodePoint(2);
				break;
			}
			if (checkForValidEscape(context.code, context.nextCode)) {
				context.consumeCodePoint();
				consumeEscapedCodePoint(context);
			}
			break;
		default: context.consumeCodePoint();
	}
};
/**
* @file Tokenizing logic for whitespace
*/
/**
* § 4.3.1. Consume a token (whitespace)
*
* @see {@link https://www.w3.org/TR/css-syntax-3/#consume-token}
* @param context Reference to the tokenizer context instance
*/
var consumeWhitespaceToken = (context) => {
	const start = context.offset;
	context.consumeWhitespace();
	context.onToken(TokenType.Whitespace, start, context.offset, void 0, context.stop);
};
/**
* @file CSS tokenizer that strictly follows the CSS Syntax Module Level 3 specification
*
* @see {@link https://www.w3.org/TR/css-syntax-3/#tokenization}
*/
/**
* CSS tokenizer function
*
* @param source Source code to tokenize
* @param onToken Tokenizer callback which is called for each token found in source code
* @param onError Error callback which is called when a parsing error is found (optional)
* @param functionHandlers Custom function handlers (optional)
*/
var tokenize = (source, onToken, onError = () => {}, functionHandlers) => {
	const context = new TokenizerContext(source, onToken, onError, functionHandlers);
	while (!context.isEof()) switch (context.code) {
		case 9:
		case 32:
		case 10:
		case 12:
		case 13:
			consumeWhitespaceToken(context);
			break;
		case 48:
		case 49:
		case 50:
		case 51:
		case 52:
		case 53:
		case 54:
		case 55:
		case 56:
		case 57:
			consumeNumericToken(context);
			break;
		case 40:
			context.consumeTrivialToken(TokenType.OpenParenthesis);
			break;
		case 41:
			context.consumeTrivialToken(TokenType.CloseParenthesis);
			break;
		case 44:
			context.consumeTrivialToken(TokenType.Comma);
			break;
		case 58:
			context.consumeTrivialToken(TokenType.Colon);
			break;
		case 59:
			context.consumeTrivialToken(TokenType.Semicolon);
			break;
		case 91:
			context.consumeTrivialToken(TokenType.OpenSquareBracket);
			break;
		case 93:
			context.consumeTrivialToken(TokenType.CloseSquareBracket);
			break;
		case 123:
			context.consumeTrivialToken(TokenType.OpenCurlyBracket);
			break;
		case 125:
			context.consumeTrivialToken(TokenType.CloseCurlyBracket);
			break;
		case 39:
		case 34:
			consumeStringToken(context);
			break;
		case 35:
			if (isIdentCodePoint(context.getRelativeCode(1)) || checkForValidEscape(context.getRelativeCode(1), context.getRelativeCode(2))) {
				const start = context.offset;
				context.consumeCodePoint();
				consumeIndentSequence(context);
				context.onToken(TokenType.Hash, start, context.offset, void 0, context.stop);
				break;
			}
			context.consumeTrivialToken(TokenType.Delim);
			break;
		case 43:
			if (checkForNumberStart(context.code, context.getRelativeCode(1), context.getRelativeCode(2))) {
				consumeNumericToken(context);
				break;
			}
			context.consumeTrivialToken(TokenType.Delim);
			break;
		case 45:
			if (checkForNumberStart(context.code, context.getRelativeCode(1), context.getRelativeCode(2))) {
				consumeNumericToken(context);
				break;
			}
			if (context.getRelativeCode(1) === 45 && context.getRelativeCode(2) === 62) {
				context.consumeCodePoint(3);
				context.onToken(TokenType.Cdc, context.offset - 3, context.offset, void 0, context.stop);
				break;
			}
			if (checkForIdentStart(context.code, context.getRelativeCode(1), context.getRelativeCode(2))) {
				consumeIdentLikeToken(context);
				break;
			}
			context.consumeTrivialToken(TokenType.Delim);
			break;
		case 46:
			if (checkForNumberStart(context.code, context.getRelativeCode(1), context.getRelativeCode(2))) {
				consumeNumericToken(context);
				break;
			}
			context.consumeTrivialToken(TokenType.Delim);
			break;
		case 60:
			if (context.getRelativeCode(1) === 33 && context.getRelativeCode(2) === 45 && context.getRelativeCode(3) === 45) {
				context.consumeCodePoint(4);
				context.onToken(TokenType.Cdo, context.offset - 4, context.offset, void 0, context.stop);
				break;
			}
			context.consumeTrivialToken(TokenType.Delim);
			break;
		case 64:
			if (checkForIdentStart(context.getRelativeCode(1), context.getRelativeCode(2), context.getRelativeCode(3))) {
				const start = context.offset;
				context.consumeCodePoint();
				consumeIndentSequence(context);
				context.onToken(TokenType.AtKeyword, start, context.offset, void 0, context.stop);
				break;
			}
			context.consumeTrivialToken(TokenType.Delim);
			break;
		case 92:
			if (checkForValidEscape(context.code, context.getRelativeCode(1))) {
				consumeIdentLikeToken(context);
				break;
			}
			context.consumeTrivialToken(TokenType.Delim);
			context.onError("Invalid escape sequence.", context.offset - 1, context.offset);
			break;
		case 47:
			if (context.getRelativeCode(1) === 42) {
				const start = context.offset;
				context.consumeCodePoint(2);
				context.consumeUntilCommentEnd();
				if (context.isEof()) context.onError("Unterminated comment.", start, context.length - 2);
				context.onToken(TokenType.Comment, start, context.offset, void 0, context.stop);
				break;
			}
			context.consumeTrivialToken(TokenType.Delim);
			break;
		default:
			if (isIdentStartCodePoint(context.code)) {
				consumeIdentLikeToken(context);
				break;
			}
			context.consumeTrivialToken(TokenType.Delim);
	}
};
/**
* @file Custom tokenizing logic for Extended CSS's pseudo-classes
*/
/**
* Generic handler for the Extended CSS's pseudo-classes
*
* @param context Reference to the tokenizer context instance
*/
var handleRegularExtendedCssPseudo = (context) => {
	const start = context.offset;
	context.consumeWhitespace();
	if (context.code === 39 || context.code === 34) {
		if (context.offset > start) context.onToken(TokenType.Whitespace, start, context.offset, void 0, context.stop);
		return;
	}
	let balance = 1;
	for (let i = start; i < context.offset; i += 1) context.onToken(TokenType.Delim, i, i + 1, void 0, context.stop);
	while (!context.isEof()) {
		if (context.code === 40 && context.prevCode !== 92) balance += 1;
		else if (context.code === 41 && context.prevCode !== 92) {
			balance -= 1;
			if (balance === 0) break;
		}
		context.consumeTrivialToken(TokenType.Delim);
	}
};
/**
* @file Custom tokenizing logic for Extended CSS's `:xpath()` pseudo-class
* @note `:xpath()` is a bit tricky, because it can contain unescaped parentheses inside strings in the XPath
* expression.
*/
/**
* Handler for the Extended CSS's `:xpath()` pseudo-class
*
* @param context Reference to the tokenizer context instance
*/
var handleXpathExtendedCssPseudo = (context) => {
	const start = context.offset;
	context.consumeWhitespace();
	if (context.code === 39 || context.code === 34) {
		if (context.offset > start) context.onToken(TokenType.Whitespace, start, context.offset, void 0, context.stop);
		return;
	}
	let balance = 1;
	for (let i = start; i < context.offset; i += 1) context.onToken(TokenType.Delim, i, i + 1, void 0, context.stop);
	let inString = false;
	while (!context.isEof()) {
		if (context.code === 34 && context.prevCode !== 92) inString = !inString;
		if (!inString) {
			if (context.code === 40 && context.prevCode !== 92) balance += 1;
			else if (context.code === 41 && context.prevCode !== 92) {
				balance -= 1;
				if (balance === 0) break;
			}
		}
		context.consumeTrivialToken(TokenType.Delim);
	}
};
/**
* @file Map utility functions
*/
/**
* Simple utility function to merge two maps.
*
* @param map1 First map
* @param map2 Second map
* @returns Merged map
* @note If a key is present in both maps, the value from the second map will be used
* @note This function does not modify the original maps, it returns a new map
*/
function mergeMaps(map1, map2) {
	const result = /* @__PURE__ */ new Map();
	for (const [key, value] of map1) result.set(key, value);
	for (const [key, value] of map2) result.set(key, value);
	return result;
}
/**
* Map of Extended CSS's pseudo-classes and their respective handler functions
*/
var EXT_CSS_PSEUDO_HANDLERS = new Map([
	[1989084725, handleRegularExtendedCssPseudo],
	[2399470598, handleRegularExtendedCssPseudo],
	[1221663855, handleRegularExtendedCssPseudo],
	[102304302, handleRegularExtendedCssPseudo],
	[2923888231, handleRegularExtendedCssPseudo],
	[1739713050, handleRegularExtendedCssPseudo],
	[1860790666, handleRegularExtendedCssPseudo],
	[3376104318, handleRegularExtendedCssPseudo],
	[196571984, handleXpathExtendedCssPseudo]
]);
/**
* Extended CSS tokenizer function
*
* @param source Source code to tokenize
* @param onToken Tokenizer callback which is called for each token found in source code
* @param onError Error callback which is called when a parsing error is found (optional)
* @param functionHandlers Custom function handlers (optional)
* @note If you specify custom function handlers, they will be merged with the default function handlers. If you
* duplicate a function handler, the custom one will be used instead of the default one, so you can override the default
* function handlers this way, if you want to.
*/
var tokenizeExtended = (source, onToken, onError = () => {}, functionHandlers = /* @__PURE__ */ new Map()) => {
	tokenize(source, onToken, onError, functionHandlers.size > 0 ? mergeMaps(EXT_CSS_PSEUDO_HANDLERS, functionHandlers) : EXT_CSS_PSEUDO_HANDLERS);
};
/**
* Checks if the given raw string contains any of the specified tokens.
*
* @param raw - The raw string to be tokenized and checked.
* @param tokens - A set of token types to check for in the raw string.
* @param tokenizer - The tokenizer function to use. Defaults to `tokenizeExtended`.
* @returns `true` if any of the specified tokens are found in the raw string, otherwise `false`.
*/
var hasToken = (raw, tokens, tokenizer = tokenizeExtended) => {
	let found = false;
	tokenizer(raw, (type, start, end, props, stop) => {
		if (tokens.has(type)) {
			found = true;
			stop();
		}
	});
	return found;
};
//#endregion
export { TokenType, getFormattedTokenName, hasToken, tokenizeExtended };
