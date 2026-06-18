import { AdblockSyntaxError } from "../../errors/adblock-syntax-error.js";
import { require_sprintf } from "../../../../../sprintf-js/src/sprintf.js";
import { TokenType, getFormattedTokenName, tokenizeExtended } from "../../../../css-tokenizer/dist/csstokenizer.js";
import { END_OF_INPUT, ERROR_MESSAGES } from "./constants.js";
//#region node_modules/@adguard/agtree/dist/parser/css/balancing.js
var import_sprintf = require_sprintf();
/**
* @file Tokenizer helpers for balanced pairs.
*/
/**
* Map of opening tokens to their corresponding closing tokens.
*/
var standardTokenPairs = new Map([
	[TokenType.Function, TokenType.CloseParenthesis],
	[TokenType.OpenParenthesis, TokenType.CloseParenthesis],
	[TokenType.OpenSquareBracket, TokenType.CloseSquareBracket],
	[TokenType.OpenCurlyBracket, TokenType.CloseCurlyBracket]
]);
/**
* Map of opening tokens to their corresponding closing tokens just for function calls. This makes possible a more
* lightweight and tolerant check for balanced pairs in some cases.
*/
var functionTokenPairs = new Map([[TokenType.Function, TokenType.CloseParenthesis], [TokenType.OpenParenthesis, TokenType.CloseParenthesis]]);
/**
* Helper function to tokenize and ensure balanced pairs.
*
* @param raw Raw CSS string to tokenize
* @param onToken Callback which will be invoked for each token, extended with a `balance` parameter
* @param onError Error callback which is called when a parsing error is found (optional)
* @param functionHandlers Custom function handlers (optional)
* @param tokenPairs Map of opening tokens to their corresponding closing tokens
* @throws If the input is not balanced
* @todo Consider adding a `tolerant` flag if error throwing seems too aggressive in the future
*/
var tokenizeWithBalancedPairs = (raw, onToken, onError = () => {}, functionHandlers, tokenPairs = standardTokenPairs) => {
	const stack = [];
	const values = new Set(tokenPairs.values());
	tokenizeExtended(raw, (type, start, end, props, stop) => {
		if (tokenPairs.has(type)) stack.push(tokenPairs.get(type));
		else if (values.has(type)) if (stack[stack.length - 1] === type) stack.pop();
		else throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.EXPECTED_TOKEN_BUT_GOT, getFormattedTokenName(stack[stack.length - 1]), getFormattedTokenName(type)), start, raw.length);
		onToken(type, start, end, props, stack.length, stop);
	}, onError, functionHandlers);
	if (stack.length > 0) throw new AdblockSyntaxError((0, import_sprintf.sprintf)(ERROR_MESSAGES.EXPECTED_TOKEN_BUT_GOT, getFormattedTokenName(stack[stack.length - 1]), END_OF_INPUT), raw.length - 1, raw.length);
};
/**
* Tokenize and ensure balanced pairs for standard CSS.
*
* @param raw Raw CSS string to tokenize
* @param onToken Callback which will be invoked for each token, extended with a `balance` parameter
* @param onError Error callback which is called when a parsing error is found (optional)
* @param functionHandlers Custom function handlers (optional)
* @throws If the input is not balanced
*/
var tokenizeBalanced = (raw, onToken, onError = () => {}, functionHandlers) => {
	tokenizeWithBalancedPairs(raw, onToken, onError, functionHandlers);
};
/**
* Tokenize and ensure balanced pairs for function calls.
*
* @param raw Raw CSS string to tokenize
* @param onToken Callback which will be invoked for each token, extended with a `balance` parameter
* @param onError Error callback which is called when a parsing error is found (optional)
* @param functionHandlers Custom function handlers (optional)
* @throws If the input is not balanced
*/
var tokenizeFnBalanced = (raw, onToken, onError = () => {}, functionHandlers) => {
	tokenizeWithBalancedPairs(raw, onToken, onError, functionHandlers, functionTokenPairs);
};
//#endregion
export { tokenizeBalanced, tokenizeFnBalanced };
