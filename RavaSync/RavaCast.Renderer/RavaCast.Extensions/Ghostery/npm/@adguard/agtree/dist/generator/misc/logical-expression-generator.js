import { OperatorValue } from "../../nodes/index.js";
import { NodeType } from "../../parser/misc/logical-expression-parser.js";
import { BaseGenerator } from "../base-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/misc/logical-expression-generator.js
/**
* Generator for logical expression nodes.
*/
var LogicalExpressionGenerator = class LogicalExpressionGenerator extends BaseGenerator {
	/**
	* Generates a string representation of the logical expression (serialization).
	*
	* @param node Expression node
	* @returns String representation of the logical expression
	*/
	static generate(node) {
		if (node.type === NodeType.Variable) return node.name;
		if (node.type === NodeType.Operator) {
			const left = LogicalExpressionGenerator.generate(node.left);
			const right = node.right ? LogicalExpressionGenerator.generate(node.right) : void 0;
			const { operator } = node;
			if (operator === OperatorValue.Not) return `${operator}${left}`;
			if (!right) throw new Error("Expected right operand");
			return `${left} ${operator} ${right}`;
		}
		if (node.type === NodeType.Parenthesis) return `(${LogicalExpressionGenerator.generate(node.expression)})`;
		throw new Error("Unexpected node type");
	}
};
//#endregion
export { LogicalExpressionGenerator };
