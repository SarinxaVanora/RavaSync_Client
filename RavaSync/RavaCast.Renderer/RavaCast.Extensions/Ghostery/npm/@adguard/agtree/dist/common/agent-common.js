import { AdblockSyntax } from "../utils/adblockers.js";
//#region node_modules/@adguard/agtree/dist/common/agent-common.js
/**
* Possible AdGuard agent markers.
*/
var ADG_NAME_MARKERS = new Set(["adguard", "adg"]);
/**
* Possible uBlock Origin agent markers.
*/
var UBO_NAME_MARKERS = new Set([
	"ublock",
	"ublock origin",
	"ubo"
]);
/**
* Possible Adblock Plus agent markers.
*/
var ABP_NAME_MARKERS = new Set([
	"adblock",
	"adblock plus",
	"adblockplus",
	"abp"
]);
/**
* Returns the adblock syntax based on the adblock name parsed from the agent type comment.
* Needed for modifiers validation of network rules by AGLint.
*
* @param name Adblock name.
*
* @returns Adblock syntax.
*/
var getAdblockSyntax = (name) => {
	let syntax = AdblockSyntax.Common;
	const lowerCaseName = name.toLowerCase();
	if (ADG_NAME_MARKERS.has(lowerCaseName)) syntax = AdblockSyntax.Adg;
	else if (UBO_NAME_MARKERS.has(lowerCaseName)) syntax = AdblockSyntax.Ubo;
	else if (ABP_NAME_MARKERS.has(lowerCaseName)) syntax = AdblockSyntax.Abp;
	return syntax;
};
//#endregion
export { getAdblockSyntax };
