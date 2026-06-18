import mappings_default from "../mappings.js";
//#region node_modules/@ghostery/urlfilter2dnr/dist/esm/converters/helpers.js
var typedResourceMapping = mappings_default;
var FIRST_OPTION_PATTERN = /\$[a-z0-9-]+(?:[,=]|$)/;
var DEFAULT_PARAM_MAPPING = {
	"3p": "third-party",
	xhr: "xmlhttprequest",
	frame: "subdocument",
	from: "domain"
};
/**
* Finds the index of first filter option
* @param filter The filter line
* @returns The index of '$' sign followed by options
*/
function findOptionIndex(filter) {
	const match = FIRST_OPTION_PATTERN.exec(filter);
	if (match === null) return -1;
	return match.index;
}
/**
* Translates resource name
* @param {string} name The source resource name
* @param {'ubo' | 'adg'} dialect The destination dialect
* @returns Translated dialect or passes given name in case of not found
*/
function convertName(name, dialect) {
	return typedResourceMapping[name]?.[dialect] ?? name;
}
/**
* Replaces `$redirect` and `$redirect-rule` option value with preferred dialect
* @param {string} line line The filter line
* @param {'ubo' | 'adg'} dialect The destination dialect
* @returns A filter line after resource translation
*/
function convertRedirectFilterOptions(line, dialect) {
	const normalizeFilterProperty = (line, property) => {
		const redirectStartsAt = line.indexOf(`${property}=`);
		if (redirectStartsAt === -1) return line;
		let redirectEndsAt = line.indexOf(",", redirectStartsAt);
		if (redirectEndsAt === -1) redirectEndsAt = line.length;
		return `${line.slice(0, redirectStartsAt)}${property}=${convertName(line.slice(redirectStartsAt + property.length + 1, redirectEndsAt), dialect)}${line.slice(redirectEndsAt)}`;
	};
	if (!line.includes("redirect")) return line;
	line = normalizeFilterProperty(line, "redirect");
	line = normalizeFilterProperty(line, "redirect-rule");
	return line;
}
function normalizeFilter(filter, { mapping = DEFAULT_PARAM_MAPPING } = {}) {
	filter = convertRedirectFilterOptions(filter, "adg");
	const index = findOptionIndex(filter);
	if (index === -1) {
		if (!(filter.startsWith("/") && filter.endsWith("/"))) return filter.toLowerCase();
		return filter;
	}
	let front = filter.slice(0, index);
	let params = filter.slice(index + 1).split(",").join(",").split(",");
	params.forEach((param, index) => {
		const [key, value] = param.split("=");
		const alias = mapping[key];
		if (alias) params[index] = value ? `${alias}=${value}` : alias;
	});
	params = params.filter((param, index) => {
		return params.indexOf(param) === index;
	});
	if (!(front.startsWith("/") && front.endsWith("/")) && !params.find((p) => p === "match-case")) front = front?.toLowerCase();
	return `${front}$${params.join(",")}`;
}
function normalizeRule(rule, { resourcesPath = "", id } = {}) {
	if (!rule) return;
	const newRule = structuredClone(rule);
	if (id) newRule.id = id;
	if (newRule.condition.urlFilter?.endsWith("*")) newRule.condition.urlFilter = newRule.condition.urlFilter.slice(0, -1);
	if (!newRule.condition.urlFilter) delete newRule.condition.urlFilter;
	if (rule.condition.isUrlFilterCaseSensitive !== true) delete newRule.condition.isUrlFilterCaseSensitive;
	if (newRule.condition.regexFilter?.startsWith("/") && newRule.condition.regexFilter?.endsWith("/")) newRule.condition.regexFilter = newRule.condition.regexFilter.slice(1, -1);
	if (newRule.condition.excludedDomains) {
		newRule.condition.excludedInitiatorDomains = newRule.condition.excludedDomains;
		delete newRule.condition.excludedDomains;
	}
	if (newRule.condition.domains) {
		newRule.condition.initiatorDomains = newRule.condition.domains;
		delete newRule.condition.domains;
	}
	if (newRule.action.type === "redirect") {
		if (newRule.action.redirect?.extensionPath) {
			const resourceName = newRule.action.redirect.extensionPath.slice(resourcesPath.length + 1);
			newRule.action.redirect.extensionPath = `${resourcesPath}/${convertName(resourceName, "ubo")}`;
		}
	}
	return newRule;
}
//#endregion
export { normalizeFilter, normalizeRule };
