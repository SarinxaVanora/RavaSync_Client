//#region node_modules/@adguard/agtree/dist/converter/base-interfaces/conversion-result.js
/**
* @file Conversion result interface and helper functions
*/
/**
* Helper function to create a generic conversion result.
*
* @param result Conversion result
* @param isConverted Indicates whether the input item was converted
* @template T Type of the item to convert
* @template U Type of the conversion result (defaults to `T`, but can be `T[]` as well)
* @returns Generic conversion result
*/
function createConversionResult(result, isConverted) {
	return {
		result,
		isConverted
	};
}
/**
* Helper function to create a node conversion result.
*
* @param nodes Array of nodes
* @param isConverted Indicates whether the input item was converted
* @template T Type of the node (extends `Node`)
* @returns Node conversion result
*/
function createNodeConversionResult(nodes, isConverted) {
	return createConversionResult(nodes, isConverted);
}
//#endregion
export { createConversionResult, createNodeConversionResult };
