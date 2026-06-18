//#region node_modules/@adguard/agtree/dist/converter/data/css.js
/**
* @file Known CSS elements and attributes.
* TODO: Implement a compatibility table for Extended CSS
*/
/**
* Legacy Extended CSS attribute prefix.
*
* @example
* ```css
* [-ext-<name>=...]
* ```
*/
var LEGACY_EXT_CSS_ATTRIBUTE_PREFIX = "-ext-";
/**
* ABP Extended CSS prefix.
*
* @example
* ```css
* [-abp-<name>=...]
* -abp-<name>(...)
* ```
*/
var ABP_EXT_CSS_PREFIX = "-abp";
/**
* Known CSS pseudo-classes that are supported by all browsers natively,
* but can also be applied as extended.
*/
var NATIVE_CSS_PSEUDO_CLASSES = new Set([
	"has",
	"is",
	"not"
]);
/**
* Known _strict_ Extended CSS pseudo-classes. Please, keep this list sorted.
* Strict means that these pseudo-classes are not supported by any browser natively,
* and they always require Extended CSS libraries to work.
*/
var EXT_CSS_PSEUDO_CLASSES_STRICT = new Set([
	"contains",
	"if-not",
	"matches-attr",
	"matches-css",
	"matches-property",
	"nth-ancestor",
	"remove",
	"upward",
	"xpath",
	"has-text",
	"matches-css-after",
	"matches-css-before",
	"matches-path",
	"min-text-length",
	"watch-attr",
	"-abp-contains",
	"-abp-has",
	"-abp-properties"
]);
/**
* _ALL_ known Extended CSS pseudo-classes. Please, keep this list sorted.
* It includes strict pseudo-classes and additional pseudo-classes that may be
* supported by some browsers natively.
*/
var EXT_CSS_PSEUDO_CLASSES = new Set([...EXT_CSS_PSEUDO_CLASSES_STRICT, ...NATIVE_CSS_PSEUDO_CLASSES]);
/**
* Known extended CSS property that is used to remove elements.
*
* @see {@link https://github.com/AdguardTeam/ExtendedCss#remove-pseudos}
*/
var REMOVE_PROPERTY = "remove";
/**
* Known extended CSS value for {@link REMOVE_PROPERTY} property to remove elements.
*
* @see {@link https://github.com/AdguardTeam/ExtendedCss#remove-pseudos}
*/
var REMOVE_VALUE = "true";
//#endregion
export { ABP_EXT_CSS_PREFIX, EXT_CSS_PSEUDO_CLASSES, EXT_CSS_PSEUDO_CLASSES_STRICT, LEGACY_EXT_CSS_ATTRIBUTE_PREFIX, NATIVE_CSS_PSEUDO_CLASSES, REMOVE_PROPERTY, REMOVE_VALUE };
