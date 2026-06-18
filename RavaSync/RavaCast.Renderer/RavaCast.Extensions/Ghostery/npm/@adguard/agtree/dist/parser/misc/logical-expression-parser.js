import { StringUtils } from "../../utils/string.js";
import { OperatorValue } from "../../nodes/index.js";
import { AdblockSyntaxError } from "../../errors/adblock-syntax-error.js";
import { BaseParser } from "../base-parser.js";
import { defaultParserOptions } from "../options.js";
//#region node_modules/@adguard/agtree/dist/parser/misc/logical-expression-parser.js
/**
* Possible token types in the logical expression.
*/
var TokenType = {
	Variable: 0,
	Operator: 1,
	Parenthesis: 2
};
/**
* Possible node types in the logical expression.
*/
var NodeType = {
	Variable: "Variable",
	Operator: "Operator",
	Parenthesis: "Parenthesis"
};
/**
* Precedence of the operators, larger number means higher precedence.
*/
var OPERATOR_PRECEDENCE = {
	[OperatorValue.Not]: 3,
	[OperatorValue.And]: 2,
	[OperatorValue.Or]: 1
};
/**
* `LogicalExpressionParser` is responsible for parsing logical expressions.
*
* @example
* From the following rule:
* ```adblock
* !#if (adguard_ext_android_cb || adguard_ext_safari)
* ```
* this parser will parse the expression `(adguard_ext_android_cb || adguard_ext_safari)`.
*/
var LogicalExpressionParser = class LogicalExpressionParser extends BaseParser {
	/**
	* Split the expression into tokens.
	*
	* @param raw Source code of the expression
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @returns Token list
	* @throws {AdblockSyntaxError} If the expression is invalid
	*/
	static tokenize(raw, baseOffset = 0) {
		const tokens = [];
		let offset = 0;
		while (offset < raw.length) {
			const char = raw[offset];
			if (StringUtils.isWhitespace(char)) offset += 1;
			else if (StringUtils.isLetter(char)) {
				const nameStart = offset;
				while (offset + 1 < raw.length && (StringUtils.isAlphaNumeric(raw[offset + 1]) || raw[offset + 1] === "_")) offset += 1;
				tokens.push({
					type: TokenType.Variable,
					start: nameStart,
					end: offset + 1
				});
				offset += 1;
			} else if (char === "(" || char === ")") {
				tokens.push({
					type: TokenType.Parenthesis,
					start: offset,
					end: offset + 1
				});
				offset += 1;
			} else if (char === "&" || char === "|") if (offset + 1 < raw.length && raw[offset + 1] === char) {
				tokens.push({
					type: TokenType.Operator,
					start: offset,
					end: offset + 2
				});
				offset += 2;
			} else throw new AdblockSyntaxError(`Unexpected character "${char}"`, baseOffset + offset, baseOffset + offset + 1);
			else if (char === "!") {
				tokens.push({
					type: TokenType.Operator,
					start: offset,
					end: offset + 1
				});
				offset += 1;
			} else throw new AdblockSyntaxError(`Unexpected character "${char}"`, baseOffset + offset, baseOffset + offset + 1);
		}
		return tokens;
	}
	/**
	* Parses a logical expression.
	*
	* @param raw Raw input to parse.
	* @param options Global parser options.
	* @param baseOffset Starting offset of the input. Node locations are calculated relative to this offset.
	* @returns Parsed expression
	* @throws {AdblockSyntaxError} If the expression is invalid
	*/
	static parse(raw, options = defaultParserOptions, baseOffset = 0) {
		const tokens = LogicalExpressionParser.tokenize(raw, baseOffset);
		let tokenIndex = 0;
		/**
		* Consumes a token of the expected type.
		*
		* @param type Expected token type
		* @returns The consumed token
		*/
		function consume(type) {
			const token = tokens[tokenIndex];
			if (!token) throw new AdblockSyntaxError(`Expected token of type "${type}", but reached end of input`, baseOffset, baseOffset + raw.length);
			// istanbul ignore next
			if (token.type !== type) throw new AdblockSyntaxError(`Expected token of type "${type}", but got "${token.type}"`, baseOffset + token.start, baseOffset + token.end);
			tokenIndex += 1;
			return token;
		}
		/**
		* Parses a variable.
		*
		* @returns Variable node
		*/
		function parseVariable() {
			const token = consume(TokenType.Variable);
			const result = {
				type: NodeType.Variable,
				name: raw.slice(token.start, token.end)
			};
			if (options.isLocIncluded) {
				result.start = baseOffset + token.start;
				result.end = baseOffset + token.end;
			}
			return result;
		}
		/**
		* Parses a binary expression.
		*
		* @param left Left-hand side of the expression
		* @param minPrecedence Minimum precedence of the operator
		* @returns Binary expression node
		*/
		function parseBinaryExpression(left, minPrecedence = 0) {
			let node = left;
			let operatorToken;
			while (tokens[tokenIndex]) {
				operatorToken = tokens[tokenIndex];
				if (!operatorToken || operatorToken.type !== TokenType.Operator) break;
				const operator = raw.slice(operatorToken.start, operatorToken.end);
				const precedence = OPERATOR_PRECEDENCE[operator];
				if (precedence < minPrecedence) break;
				tokenIndex += 1;
				const right = parseExpression(precedence + 1);
				const newNode = {
					type: NodeType.Operator,
					operator,
					left: node,
					right
				};
				if (options.isLocIncluded) {
					newNode.start = node.start ?? baseOffset + operatorToken.start;
					newNode.end = right.end ?? baseOffset + operatorToken.end;
				}
				node = newNode;
			}
			return node;
		}
		/**
		* Parses a parenthesized expression.
		*
		* @returns Parenthesized expression node
		*/
		function parseParenthesizedExpression() {
			consume(TokenType.Parenthesis);
			const expression = parseExpression();
			consume(TokenType.Parenthesis);
			const result = {
				type: NodeType.Parenthesis,
				expression
			};
			if (options.isLocIncluded) {
				result.start = expression.start;
				result.end = expression.end;
			}
			return result;
		}
		/**
		* Parses an expression.
		*
		* @param minPrecedence Minimum precedence of the operator
		* @returns Expression node
		*/
		function parseExpression(minPrecedence = 0) {
			let node;
			const token = tokens[tokenIndex];
			const value = raw.slice(token.start, token.end);
			if (token.type === TokenType.Variable) node = parseVariable();
			else if (token.type === TokenType.Operator && value === OperatorValue.Not) {
				tokenIndex += 1;
				const expression = parseExpression(OPERATOR_PRECEDENCE[OperatorValue.Not]);
				node = {
					type: NodeType.Operator,
					operator: OperatorValue.Not,
					left: expression
				};
				if (options.isLocIncluded) if (expression.end) {
					node.start = baseOffset + token.start;
					node.end = expression.end;
				} else {
					node.start = baseOffset + token.start;
					node.end = baseOffset + token.end;
				}
			} else if (token.type === TokenType.Parenthesis && value === "(") node = parseParenthesizedExpression();
			else throw new AdblockSyntaxError(`Unexpected token "${value}"`, baseOffset + token.start, baseOffset + token.end);
			return parseBinaryExpression(node, minPrecedence);
		}
		const expression = parseExpression();
		if (tokenIndex !== tokens.length) throw new AdblockSyntaxError(`Unexpected token "${tokens[tokenIndex].type}"`, baseOffset + tokens[tokenIndex].start, baseOffset + tokens[tokenIndex].end);
		return expression;
	}
};
//#endregion
export { LogicalExpressionParser, NodeType };
