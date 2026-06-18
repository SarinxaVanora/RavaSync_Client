import { __commonJSMin } from "../../../../../../virtual/_rolldown/runtime.js";
import { require_type } from "../../type.js";
import { require_esprima } from "../../../../../esprima/dist/esprima.js";
//#region node_modules/js-yaml/lib/js-yaml/type/js/function.js
var require_function = /* @__PURE__ */ __commonJSMin(((exports, module) => {
	var esprima;
	try {
		esprima = require_esprima();
	} catch (_) {
		if (typeof window !== "undefined") esprima = window.esprima;
	}
	var Type = require_type();
	function resolveJavascriptFunction(data) {
		if (data === null) return false;
		try {
			var source = "(" + data + ")", ast = esprima.parse(source, { range: true });
			if (ast.type !== "Program" || ast.body.length !== 1 || ast.body[0].type !== "ExpressionStatement" || ast.body[0].expression.type !== "ArrowFunctionExpression" && ast.body[0].expression.type !== "FunctionExpression") return false;
			return true;
		} catch (err) {
			return false;
		}
	}
	function constructJavascriptFunction(data) {
		var source = "(" + data + ")", ast = esprima.parse(source, { range: true }), params = [], body;
		if (ast.type !== "Program" || ast.body.length !== 1 || ast.body[0].type !== "ExpressionStatement" || ast.body[0].expression.type !== "ArrowFunctionExpression" && ast.body[0].expression.type !== "FunctionExpression") throw new Error("Failed to resolve function");
		ast.body[0].expression.params.forEach(function(param) {
			params.push(param.name);
		});
		body = ast.body[0].expression.body.range;
		if (ast.body[0].expression.body.type === "BlockStatement") return new Function(params, source.slice(body[0] + 1, body[1] - 1));
		return new Function(params, "return " + source.slice(body[0], body[1]));
	}
	function representJavascriptFunction(object) {
		return object.toString();
	}
	function isFunction(object) {
		return Object.prototype.toString.call(object) === "[object Function]";
	}
	module.exports = new Type("tag:yaml.org,2002:js/function", {
		kind: "scalar",
		resolve: resolveJavascriptFunction,
		construct: constructJavascriptFunction,
		predicate: isFunction,
		represent: representJavascriptFunction
	});
}));
//#endregion
export { require_function };
