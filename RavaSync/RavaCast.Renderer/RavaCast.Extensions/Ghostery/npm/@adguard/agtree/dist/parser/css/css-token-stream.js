import { AdblockSyntaxError } from "../../errors/adblock-syntax-error.js";
import { require_sprintf } from "../../../../../sprintf-js/src/sprintf.js";
import { TokenType, getFormattedTokenName, tokenizeExtended } from "../../../../css-tokenizer/dist/csstokenizer.js";
import { END_OF_INPUT, ERROR_MESSAGES } from "./constants.js";
import { tokenizeBalanced } from "./balancing.js";
import { EXT_CSS_PSEUDO_CLASSES, EXT_CSS_PSEUDO_CLASSES_STRICT, NATIVE_CSS_PSEUDO_CLASSES } from "../../converter/data/css.js";
//#region node_modules/@adguard/agtree/dist/parser/css/css-token-stream.js
var import_sprintf = require_sprintf();
/**
* @file CSS token stream.
*/
/**
* Represents a stream of CSS tokens.
*/
var CssTokenStream = class {
	/**
	* The tokens in the stream.
	*/
	tokens = [];
	/**
	* The source string.
	*/
	source = "";
	/**
	* The current index in the stream.
	*/
	index = 0;
	/**
	* The base offset of the source string.
	*/
	baseOffset;
	/**
	* Initializes a new instance of the TokenStream class.
	*
	* @param source The source string to tokenize.
	* @param baseOffset The base offset of the source string.
	*/
	constructor(source, baseOffset = 0) {
		this.source = source;
		try {
			tokenizeBalanced(source, (type, start, end, _, balance) => {
				this.tokens.push({
					type,
					start,
					end,
					balance
				});
			});
		} catch (error) {
			if (error instanceof AdblockSyntaxError) {
				error.start += baseOffset;
				error.end += baseOffset;
				throw error;
			}
		}
		this.index = 0;
		this.baseOffset = baseOffset;
	}
	/**
	* Gets the number of tokens in the stream.
	*
	* @returns The number of tokens in the stream.
	*/
	get length() {
		return this.tokens.length;
	}
	/**
	* Checks if the end of the token stream is reached.
	*
	* @returns True if the end of the stream is reached, otherwise false.
	*/
	isEof() {
		return this.index >= this.tokens.length;
	}
	/**
	* Gets the token at the specified index.
	*
	* @param index The index of the token to retrieve.
	* @returns The token at the specified index or undefined if the index is out of bounds.
	*/
	get(index = this.index) {
		return this.tokens[index];
	}
	/**
	* Gets the token at the specified index or throws if no token is found at the specified index.
	*
	* @param index The index of the token to retrieve.
	* @returns The token at the specified index or undefined if the index is out of bounds.
	* @throws If no token is found at the specified index.
	*/
	getOrFail(index = this.index) {
		const token = this.get(index);
		if (!token) throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.EXPECTED_ANY_TOKEN_BUT_GOT, END_OF_INPUT), this.baseOffset + this.source.length - 1, this.baseOffset + this.source.length);
		return token;
	}
	/**
	* Gets the source fragment of the token at the specified index.
	*
	* @param index The index of the token to retrieve the fragment for.
	* @returns The source fragment of the token or an empty string if the index is out of bounds.
	*/
	fragment(index = this.index) {
		const token = this.get(index);
		if (token) return this.source.slice(token.start, token.end);
		return "";
	}
	/**
	* Moves the index to the next token and returns it.
	*
	* @returns The next token or undefined if the end of the stream is reached.
	*/
	advance() {
		if (this.isEof()) return;
		this.index += 1;
		return this.tokens[this.index];
	}
	/**
	* Looks ahead in the stream without changing the index.
	*
	* @param index The relative index to look ahead to, starting from the current index.
	* @returns The next token or undefined if the end of the stream is reached.
	*/
	lookahead(index = 1) {
		return this.tokens[this.index + Math.max(1, index)];
	}
	/**
	* Looks behind in the stream without changing the index.
	*
	* @param index The relative index to look behind to, starting from the current index.
	* @returns The previous token or undefined if the current token is the first in the stream.
	*/
	lookbehind(index = 1) {
		if (this.index === 0) return;
		return this.tokens[this.index - Math.max(1, index)];
	}
	/**
	* Looks behind in the stream for the previous non-whitespace token without changing the index.
	*
	* @returns The previous non-whitespace token or undefined if it could not be found.
	*/
	lookbehindForNonWs() {
		for (let i = this.index - 1; i >= 0; i -= 1) if (this.tokens[i].type !== TokenType.Whitespace) return this.tokens[i];
	}
	/**
	* Skips whitespace tokens in the stream.
	*/
	skipWhitespace() {
		while (this.get()?.type === TokenType.Whitespace) this.index += 1;
	}
	/**
	* Skips tokens until the current balance level is reached.
	*
	* @returns The number of tokens skipped.
	*/
	skipUntilBalanced() {
		if (this.isEof()) return 0;
		const currentBalance = this.get().balance;
		if (currentBalance === 0) return 0;
		let skipped = 0;
		while (!this.isEof() && this.get()?.balance !== currentBalance - 1) {
			this.index += 1;
			skipped += 1;
		}
		return skipped;
	}
	/**
	* Skips tokens until a token with the specified type or the end of the stream is reached.
	*
	* @param type The type of token to skip until.
	* @param balance The balance level of the token to skip until.
	* @returns The number of tokens skipped.
	*/
	skipUntil(type, balance) {
		let skipped = 0;
		while (!this.isEof() && (this.get()?.type !== type || balance !== void 0 && this.get()?.balance !== balance)) {
			this.index += 1;
			skipped += 1;
		}
		return skipped;
	}
	/**
	* Skips tokens until a token with the specified type or the end of the stream is reached. This is an extended
	* version of skipUntil that also returns the number of tokens skipped without calculating leading and trailing
	* whitespace tokens.
	*
	* @param type The type of token to skip until.
	* @param balance The balance level of the token to skip until.
	* @returns An array containing the number of tokens skipped and the number of tokens skipped without leading and
	* trailing whitespace tokens.
	*/
	skipUntilExt(type, balance) {
		let i = this.index;
		let firstNonWsToken = -1;
		let lastNonWsToken = -1;
		while (i < this.tokens.length) {
			const currentToken = this.tokens[i];
			if (currentToken.type === TokenType.Whitespace) {
				i += 1;
				continue;
			} else if (currentToken.type === type && currentToken.balance === balance) break;
			if (firstNonWsToken === -1) firstNonWsToken = i;
			lastNonWsToken = i;
			i += 1;
		}
		const skipped = i - this.index;
		this.index = i;
		return {
			skipped,
			skippedTrimmed: firstNonWsToken === -1 ? 0 : lastNonWsToken - firstNonWsToken + 1
		};
	}
	/**
	* Expects that the end of the stream is not reached.
	*/
	expectNotEof() {
		if (this.isEof()) throw new AdblockSyntaxError("Unexpected end of input", this.baseOffset + this.source.length - 1, this.baseOffset + this.source.length);
	}
	/**
	* Expects the current token to have a specific type and optional value and balance level.
	*
	* @param type The expected token type.
	* @param data Optional expectation data.
	* @throws If the end of the stream is reached or if the token type or expectation data does not match.
	*/
	expect(type, data) {
		const token = this.get();
		if (!token) throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.EXPECTED_TOKEN_BUT_GOT, getFormattedTokenName(type), END_OF_INPUT), this.baseOffset + this.source.length - 1, this.baseOffset + this.source.length);
		if (token.type !== type) throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.EXPECTED_TOKEN_BUT_GOT, getFormattedTokenName(type), getFormattedTokenName(token.type)), this.baseOffset + token.start, this.baseOffset + token.end);
		if (data?.balance !== void 0 && token.balance !== data.balance) throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.EXPECTED_TOKEN_WITH_BALANCE_BUT_GOT, getFormattedTokenName(type), data.balance, token.balance), this.baseOffset + token.start, this.baseOffset + token.end);
		if (data?.value && this.fragment() !== data.value) throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.EXPECTED_TOKEN_WITH_VALUE_BUT_GOT, getFormattedTokenName(type), data.value, this.fragment()), this.baseOffset + token.start, this.baseOffset + token.end);
	}
	/**
	* Gets the balance level of the token at the specified index.
	*
	* @param index The index of the token to retrieve the balance level for.
	* @returns The balance level of the token or 0 if the index is out of bounds.
	*/
	getBalance(index = this.index) {
		return this.tokens[index]?.balance || 0;
	}
	/**
	* Checks whether the token stream contains any Extended CSS elements, such as `:contains()`, etc.
	*
	* @returns `true` if the stream contains any Extended CSS elements, otherwise `false`.
	*/
	hasAnySelectorExtendedCssNode() {
		return this.hasAnySelectorExtendedCssNodeInternal(EXT_CSS_PSEUDO_CLASSES);
	}
	/**
	* Strictly checks whether the token stream contains any Extended CSS elements, such as `:contains()`.
	*
	* Some Extended CSS elements are natively supported by browsers, like `:has()`.
	* This method is used to check for Extended CSS elements that are not natively supported by browsers,
	* this is why it called "strict", because it strictly checks for Extended CSS elements.
	*
	* @returns `true` if the stream contains any Extended CSS elements, otherwise `false`.
	*/
	hasAnySelectorExtendedCssNodeStrict() {
		return this.hasAnySelectorExtendedCssNodeInternal(EXT_CSS_PSEUDO_CLASSES_STRICT);
	}
	/**
	* _Lightweight_ static check for native CSS pseudo-classes — `:has()`, `:is()`, `:not()`.
	*
	* This method uses `tokenizeExtended` directly with early stopping,
	* avoiding the overhead of:
	* - full tokenization with balance tracking;
	* - storing all tokens in memory;
	* - processing remaining tokens after a match is found.
	*
	* Use this method when you only need to detect native pseudo-classes
	* and don't need the full `CssTokenStream` functionality.
	*
	* @param selector CSS selector string to check.
	* @returns True if the selector contains `:has()`, `:is()`, or `:not()`,
	* otherwise false.
	*/
	static hasNativeCssPseudoClass(selector) {
		let found = false;
		try {
			tokenizeExtended(selector, (type, start, end, _props, stop) => {
				if (type === TokenType.Function) {
					const name = selector.slice(start, end - 1);
					if (NATIVE_CSS_PSEUDO_CLASSES.has(name)) {
						found = true;
						stop();
					}
				}
			});
		} catch {
			return false;
		}
		return found;
	}
	/**
	* Checks whether the token stream contains any Extended CSS elements, such as `:has()`, `:contains()`, etc.
	*
	* @param pseudos Set of pseudo-classes to check for.
	*
	* @returns `true` if the stream contains any Extended CSS elements, otherwise `false`.
	*/
	hasAnySelectorExtendedCssNodeInternal(pseudos) {
		for (let i = 0; i < this.tokens.length; i += 1) {
			const token = this.tokens[i];
			if (token.type === TokenType.Function) {
				const name = this.source.slice(token.start, token.end - 1);
				if (pseudos.has(name)) return true;
			} else if (token.type === TokenType.OpenSquareBracket) {
				let j = i + 1;
				while (j < this.tokens.length && this.tokens[j].type === TokenType.Whitespace) j += 1;
				if (j < this.tokens.length && this.tokens[j].type === TokenType.Ident) {
					const attr = this.source.slice(this.tokens[j].start, this.tokens[j].end);
					if (attr.startsWith("-ext-") || attr.startsWith("-abp")) return true;
				}
				i = j;
			}
		}
		return false;
	}
};
//#endregion
export { CssTokenStream };
