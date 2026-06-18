import { DeclarativeFilterConverter } from "../../../../../@adguard/tsurlfilter/dist/es/declarative-converter.js";
import { normalizeFilter, normalizeRule } from "./helpers.js";
//#region node_modules/@ghostery/urlfilter2dnr/dist/esm/converters/adguard.js
var converter = new DeclarativeFilterConverter();
var SimpleFilterList = class {
	content;
	constructor(content) {
		this.content = content;
	}
	getContent() {
		return this.content;
	}
	getConversionData() {
		return {
			originals: [],
			conversions: {}
		};
	}
	getRuleText(offset) {
		if (offset >= this.content.length) return null;
		const nlIndex = this.content.indexOf("\n", offset);
		const endIndex = nlIndex === -1 ? this.content.length : nlIndex;
		const line = this.content.slice(offset, endIndex);
		return line.endsWith("\r") ? line.slice(0, -1) : line;
	}
	getOriginalRuleText(offset) {
		if (offset < 0 || offset >= this.content.length) return null;
		return this.getRuleText(offset);
	}
	getConvertedRuleOriginal() {
		return null;
	}
	getOriginalContent() {
		return this.content;
	}
};
var createFilter = (rules, filterId = 0) => {
	const filterList = new SimpleFilterList(rules.join("\n"));
	return {
		getId: () => filterId,
		getContent: async () => filterList,
		getRuleByIndex: async (index) => filterList.getOriginalRuleText(index) ?? "",
		isTrusted: () => true,
		unloadContent: () => {}
	};
};
async function convert(rules, { resourcesPath = "/prefix" } = {}) {
	if (rules.length === 0) return {
		rules: [],
		errors: [],
		limitations: []
	};
	const filter = createFilter(rules.map((rule) => normalizeFilter(rule) ?? ""));
	const conversionResult = await converter.convertStaticRuleSet(filter, { resourcesPath });
	const declarativeRules = await conversionResult.ruleSet.getDeclarativeRules();
	const normalizeRules = [];
	const errors = conversionResult.errors.map((e) => e.toString());
	for (const [index, rule] of declarativeRules.entries()) try {
		normalizeRules.push(normalizeRule(rule, {
			resourcesPath,
			id: index + 1
		}));
	} catch (e) {
		errors.push(`Could not normalize rule: ${JSON.stringify(rule)} - ${e instanceof Error ? e.message : e}`);
	}
	return {
		rules: normalizeRules,
		errors,
		limitations: conversionResult.limitations
	};
}
//#endregion
export { convert as default };
