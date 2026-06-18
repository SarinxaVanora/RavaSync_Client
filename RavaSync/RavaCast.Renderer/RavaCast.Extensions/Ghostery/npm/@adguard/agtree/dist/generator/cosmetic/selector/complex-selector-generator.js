import { BaseGenerator } from "../../base-generator.js";
import { TypeSelectorGenerator } from "./type-selector-generator.js";
import { IdSelectorGenerator } from "./id-selector-generator.js";
import { ClassSelectorGenerator } from "./class-selector-generator.js";
import { AttributeSelectorGenerator } from "./attribute-selector-generator.js";
import { PseudoClassSelectorGenerator } from "./pseudo-class-selector-generator.js";
import { SelectorCombinatorGenerator } from "./selector-combinator-generator.js";
//#region node_modules/@adguard/agtree/dist/generator/cosmetic/selector/complex-selector-generator.js
/**
* Complex selector generator.
*/
var ComplexSelectorGenerator = class extends BaseGenerator {
	/**
	* Generates a string representation of the complex selector.
	*
	* @param node Complex selector node.
	*
	* @returns String representation of the complex selector.
	*
	* @throws Error if the `node` is invalid.
	*/
	static generate(node) {
		if (node.children.length === 0) throw new Error("Complex selector cannot be empty");
		const result = [];
		for (let i = 0; i < node.children.length; i += 1) {
			const selector = node.children[i];
			const { type } = selector;
			if ((i === 0 || node.children[i - 1].type === "SelectorCombinator") && type === "SelectorCombinator") throw new Error("Empty compound selector found");
			let selectorResult;
			switch (type) {
				case "TypeSelector":
					selectorResult = TypeSelectorGenerator.generate(selector);
					break;
				case "IdSelector":
					selectorResult = IdSelectorGenerator.generate(selector);
					break;
				case "ClassSelector":
					selectorResult = ClassSelectorGenerator.generate(selector);
					break;
				case "AttributeSelector":
					selectorResult = AttributeSelectorGenerator.generate(selector);
					break;
				case "PseudoClassSelector":
					selectorResult = PseudoClassSelectorGenerator.generate(selector);
					break;
				case "SelectorCombinator":
					selectorResult = SelectorCombinatorGenerator.generate(selector);
					break;
				default: throw new Error(`Unknown selector type: ${type}`);
			}
			result.push(selectorResult);
		}
		return result.join("");
	}
};
//#endregion
export { ComplexSelectorGenerator };
