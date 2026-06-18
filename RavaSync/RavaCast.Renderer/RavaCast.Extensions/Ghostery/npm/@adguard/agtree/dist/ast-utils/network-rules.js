import { AdblockSyntax } from "../utils/adblockers.js";
import { NetworkRuleType, RuleCategory } from "../nodes/index.js";
import { isUndefined } from "../utils/type-guards.js";
import { clone } from "../utils/clone.js";
//#region node_modules/@adguard/agtree/dist/ast-utils/network-rules.js
/**
* @file Utility functions for working with network rule nodes
*/
/**
* Creates a network rule node
*
* @param pattern Rule pattern
* @param modifiers Rule modifiers (optional, default: undefined)
* @param exception Exception rule flag (optional, default: false)
* @param syntax Adblock syntax (optional, default: Common)
* @returns Network rule node
*/
function createNetworkRuleNode(pattern, modifiers = void 0, exception = false, syntax = AdblockSyntax.Common) {
	const result = {
		category: RuleCategory.Network,
		type: NetworkRuleType.NetworkRule,
		syntax,
		exception,
		pattern: {
			type: "Value",
			value: pattern
		}
	};
	if (!isUndefined(modifiers)) result.modifiers = clone(modifiers);
	return result;
}
//#endregion
export { createNetworkRuleNode };
