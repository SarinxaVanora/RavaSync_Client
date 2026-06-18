//#region node_modules/@adguard/agtree/dist/parser/options.js
/**
* @file Common options for all parsers.
*/
/**
* Default parser options.
*/
var defaultParserOptions = Object.freeze({
	tolerant: false,
	isLocIncluded: true,
	parseAbpSpecificRules: true,
	parseUboSpecificRules: true,
	includeRaws: true,
	ignoreComments: false,
	parseHostRules: false,
	parseHtmlFilteringRuleBodies: false
});
//#endregion
export { defaultParserOptions };
