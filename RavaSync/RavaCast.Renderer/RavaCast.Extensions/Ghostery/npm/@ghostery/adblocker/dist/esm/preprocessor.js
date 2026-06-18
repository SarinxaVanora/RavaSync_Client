import { sizeOfUTF8 } from "./data-view.js";
//#region node_modules/@ghostery/adblocker/dist/esm/preprocessor.js
var Env = class extends Map {};
var PreprocessorTokens;
(function(PreprocessorTokens) {
	PreprocessorTokens[PreprocessorTokens["INVALID"] = 0] = "INVALID";
	PreprocessorTokens[PreprocessorTokens["BEGIF"] = 1] = "BEGIF";
	PreprocessorTokens[PreprocessorTokens["ELSE"] = 2] = "ELSE";
	PreprocessorTokens[PreprocessorTokens["ENDIF"] = 3] = "ENDIF";
})(PreprocessorTokens || (PreprocessorTokens = {}));
function detectPreprocessor(line) {
	if (line.length < 6 || line.charCodeAt(0) !== 33 || line.charCodeAt(1) !== 35) return PreprocessorTokens.INVALID;
	if (line.startsWith("!#if ")) return PreprocessorTokens.BEGIF;
	if (line === "!#else") return PreprocessorTokens.ELSE;
	if (line === "!#endif") return PreprocessorTokens.ENDIF;
	return PreprocessorTokens.INVALID;
}
var tokenizerPattern = /(!|&&|\|\||\(|\)|[a-zA-Z0-9_]+)/g;
var identifierPattern = /^[a-zA-Z0-9_]+$/;
var tokenize = (expression) => expression.match(tokenizerPattern);
var isIdentifier = (expression) => identifierPattern.test(expression);
var precedence = {
	"!": 2,
	"&&": 1,
	"||": 0
};
var isOperator = (token) => Object.prototype.hasOwnProperty.call(precedence, token);
var testIdentifier = (identifier, env) => {
	if (identifier === "true" && !env.has("true")) return true;
	if (identifier === "false" && !env.has("false")) return false;
	return !!env.get(identifier);
};
var evaluate = (expression, env) => {
	if (expression.length === 0) return false;
	if (isIdentifier(expression)) {
		if (expression[0] === "!") return !testIdentifier(expression.slice(1), env);
		return testIdentifier(expression, env);
	}
	const tokens = tokenize(expression);
	if (!tokens || tokens.length === 0) return false;
	if (expression.length !== tokens.reduce((partialSum, token) => partialSum + token.length, 0)) return false;
	const output = [];
	const stack = [];
	for (const token of tokens) if (token === "(") stack.push(token);
	else if (token === ")") {
		while (stack.length !== 0 && stack[stack.length - 1] !== "(") output.push(stack.pop());
		if (stack.length === 0) return false;
		stack.pop();
	} else if (isOperator(token)) {
		while (stack.length && isOperator(stack[stack.length - 1]) && precedence[token] <= precedence[stack[stack.length - 1]]) output.push(stack.pop());
		stack.push(token);
	} else output.push(testIdentifier(token, env));
	if (stack.includes("(")) return false;
	while (stack.length !== 0) output.push(stack.pop());
	for (const token of output) if (token === true || token === false) stack.push(token);
	else if (token === "!") stack.push(!stack.pop());
	else if (isOperator(token)) {
		const right = stack.pop();
		const left = stack.pop();
		if (token === "&&") stack.push(left && right);
		else stack.push(left || right);
	}
	return stack[0] === true;
};
var joinConditions = (expressions) => {
	if (expressions.length === 1) return expressions[0];
	return expressions.map((expression) => `(${expression})`).join("&&");
};
var Preprocessor = class Preprocessor {
	static getCondition(line) {
		return line.slice(5).replace(/\s/g, "");
	}
	static parse(line, filterIDs) {
		return new this({
			condition: Preprocessor.getCondition(line),
			filterIDs
		});
	}
	static deserialize(view) {
		const condition = view.getUTF8();
		const filterIDs = /* @__PURE__ */ new Set();
		for (let i = 0, l = view.getUint32(); i < l; i++) filterIDs.add(view.getUint32());
		return new this({
			condition,
			filterIDs
		});
	}
	constructor({ condition, filterIDs = /* @__PURE__ */ new Set() }) {
		this.condition = condition;
		this.filterIDs = filterIDs;
	}
	evaluate(env) {
		return evaluate(this.condition, env);
	}
	serialize(view) {
		view.pushUTF8(this.condition);
		view.pushUint32(this.filterIDs.size);
		for (const filterID of this.filterIDs) view.pushUint32(filterID);
	}
	getSerializedSize() {
		let estimatedSize = sizeOfUTF8(this.condition);
		estimatedSize += (1 + this.filterIDs.size) * 4;
		return estimatedSize;
	}
};
//#endregion
export { Env, PreprocessorTokens, Preprocessor as default, detectPreprocessor, evaluate, joinConditions };
