//#region node_modules/@adguard/agtree/dist/nodes/index.js
var OperatorValue = {
	Not: "!",
	And: "&&",
	Or: "||"
};
/**
* Represents the different comment markers that can be used in an adblock rule.
*
* @example
* - If the rule is `! This is just a comment`, then the marker will be `!`.
* - If the rule is `# This is just a comment`, then the marker will be `#`.
*/
var CommentMarker = {
	/**
	* Regular comment marker. It is supported by all ad blockers.
	*/
	Regular: "!",
	/**
	* Hashmark comment marker. It is supported by uBlock Origin and AdGuard,
	* and also used in hosts files.
	*/
	Hashmark: "#"
};
/**
* Represents the main categories that an adblock rule can belong to.
* Of course, these include additional subcategories.
*/
var RuleCategory = {
	/**
	* Empty "rules" that are only containing whitespaces. These rules are handled just for convenience.
	*/
	Empty: "Empty",
	/**
	* Syntactically invalid rules (tolerant mode only).
	*/
	Invalid: "Invalid",
	/**
	* Comment rules, such as comment rules, metadata rules, preprocessor rules, etc.
	*/
	Comment: "Comment",
	/**
	* Cosmetic rules, such as element hiding rules, CSS rules, scriptlet rules, HTML rules, and JS rules.
	*/
	Cosmetic: "Cosmetic",
	/**
	* Network rules, such as basic network rules, header remover network rules, redirect network rules,
	* response header filtering rules, etc.
	*/
	Network: "Network"
};
/**
* Represents similar types of modifiers values
* which may be separated by a comma `,` (only for DomainList) or a pipe `|`.
*/
var ListNodeType = {
	AppList: "AppList",
	DomainList: "DomainList",
	MethodList: "MethodList",
	StealthOptionList: "StealthOptionList"
};
/**
* Represents child items for {@link ListNodeType}.
*/
var ListItemNodeType = {
	Unknown: "Unknown",
	App: "App",
	Domain: "Domain",
	Method: "Method",
	StealthOption: "StealthOption"
};
/**
* Represents possible comment types.
*/
var CommentRuleType = {
	AgentCommentRule: "AgentCommentRule",
	CommentRule: "CommentRule",
	ConfigCommentRule: "ConfigCommentRule",
	HintCommentRule: "HintCommentRule",
	MetadataCommentRule: "MetadataCommentRule",
	PreProcessorCommentRule: "PreProcessorCommentRule"
};
/**
* Represents possible cosmetic rule types.
*/
var CosmeticRuleType = {
	ElementHidingRule: "ElementHidingRule",
	CssInjectionRule: "CssInjectionRule",
	ScriptletInjectionRule: "ScriptletInjectionRule",
	HtmlFilteringRule: "HtmlFilteringRule",
	JsInjectionRule: "JsInjectionRule"
};
/**
* Represents possible cosmetic rule separators.
*/
var CosmeticRuleSeparator = {
	/**
	* @see {@link https://help.eyeo.com/adblockplus/how-to-write-filters#elemhide_basic}
	*/
	ElementHiding: "##",
	/**
	* @see {@link https://help.eyeo.com/adblockplus/how-to-write-filters#elemhide_basic}
	*/
	ElementHidingException: "#@#",
	/**
	* @see {@link https://help.eyeo.com/adblockplus/how-to-write-filters#elemhide_basic}
	*/
	ExtendedElementHiding: "#?#",
	/**
	* @see {@link https://help.eyeo.com/adblockplus/how-to-write-filters#elemhide_basic}
	*/
	ExtendedElementHidingException: "#@?#",
	/**
	* @see {@link https://help.eyeo.com/adblockplus/how-to-write-filters#elemhide_basic}
	*/
	AbpSnippet: "#$#",
	/**
	* @see {@link https://help.eyeo.com/adblockplus/how-to-write-filters#elemhide_basic}
	*/
	AbpSnippetException: "#@$#",
	/**
	* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#cosmetic-css-rules}
	*/
	AdgCssInjection: "#$#",
	/**
	* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#cosmetic-css-rules}
	*/
	AdgCssInjectionException: "#@$#",
	/**
	* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#cosmetic-css-rules}
	*/
	AdgExtendedCssInjection: "#$?#",
	/**
	* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#cosmetic-css-rules}
	*/
	AdgExtendedCssInjectionException: "#@$?#",
	/**
	* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#scriptlets}
	*/
	AdgJsInjection: "#%#",
	/**
	* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#scriptlets}
	*/
	AdgJsInjectionException: "#@%#",
	/**
	* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#html-filtering-rules}
	*/
	AdgHtmlFiltering: "$$",
	/**
	* @see {@link https://kb.adguard.com/en/general/how-to-create-your-own-ad-filters#html-filtering-rules}
	*/
	AdgHtmlFilteringException: "$@$"
};
/**
* Represents the different types of network rules.
*/
var NetworkRuleType = {
	NetworkRule: "NetworkRule",
	HostRule: "HostRule"
};
//#endregion
export { CommentMarker, CommentRuleType, CosmeticRuleSeparator, CosmeticRuleType, ListItemNodeType, ListNodeType, NetworkRuleType, OperatorValue, RuleCategory };
