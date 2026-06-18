import { getPlaceholder } from "./template/utils.js";
import { compile } from "./template/index.js";
//#region node_modules/hybrids/src/localize.js
var dictionary = /* @__PURE__ */ new Map();
var cache = /* @__PURE__ */ new Map();
var translate = null;
var languages = (() => {
	let list;
	// istanbul ignore next
	try {
		list = globalThis.navigator.languages || [globalThis.navigator.language];
	} catch (e) {
		list = [];
	}
	return list.reduce((set, code) => {
		const codeWithoutRegion = code.split("-")[0];
		set.add(code);
		if (code !== codeWithoutRegion) set.add(codeWithoutRegion);
		return set;
	}, /* @__PURE__ */ new Set());
})();
function isLocalizeEnabled() {
	return translate !== null || dictionary.size;
}
var pluralRules = /* @__PURE__ */ new Map();
function get(key, context, args = []) {
	key = key.trim().replace(/\s+/g, " ");
	context = context.trim();
	const cacheKey = `${key} | ${context}`;
	let msg = cache.get(cacheKey);
	if (!msg) {
		if (dictionary.size) for (const lang of languages) {
			const msgs = dictionary.get(lang);
			if (msgs) {
				msg = msgs[cacheKey] || msgs[key];
				if (msg) {
					msg = msg.message;
					if (typeof msg === "object") {
						let rules = pluralRules.get(lang);
						if (!rules) {
							rules = new Intl.PluralRules(lang);
							pluralRules.set(lang, rules);
						}
						const pluralForms = msg;
						msg = (number) => number === 0 && pluralForms.zero || pluralForms[rules.select(number)] || pluralForms.other || "";
					}
					break;
				}
			}
		}
		if (!msg) {
			if (translate) msg = translate(key, context);
			if (!msg) {
				msg = key;
				if (dictionary.size || translate) console.warn(`Missing translation: "${key}"${context ? ` [${context}]` : ""}`);
			}
		}
		cache.set(cacheKey, msg);
	}
	return typeof msg === "function" ? msg(args[0]) : msg;
}
function getKeyInChromeI18nFormat(key) {
	return key.replace("$", "@").replace(/[^a-zA-Z0-9_@]/g, "_").toLowerCase();
}
function localize(lang, messages) {
	switch (typeof lang) {
		case "function":
			if ((messages || {}).format === "chrome.i18n") {
				const cachedKeys = /* @__PURE__ */ new Map();
				translate = (key, context) => {
					key = context ? `${key} | ${context}` : key;
					let cachedKey = cachedKeys.get(key);
					if (!cachedKey) {
						cachedKey = getKeyInChromeI18nFormat(key);
						cachedKeys.set(key, cachedKey);
					}
					return lang(cachedKey, context);
				};
			} else translate = lang;
			break;
		case "string": {
			if (!messages || typeof messages !== "object") throw TypeError("Messages must be an object");
			if (lang === "default") languages.add("default");
			const current = dictionary.get(lang) || {};
			dictionary.set(lang, {
				...current,
				...messages
			});
			break;
		}
		default: throw TypeError("The first argument must be a string or a function");
	}
}
Object.defineProperty(localize, "languages", { get: () => Array.from(languages) });
function getString(parts, args) {
	let string = "";
	for (const [index, part] of parts.entries()) string += index ? `\${${index - 1}}${part}` : part;
	const [key, , context = ""] = string.split("|");
	return get(key, context, args);
}
var EXP_REGEX = /\$\{(\d+)\}/g;
function msg(parts, ...args) {
	return getString(parts, args).replace(EXP_REGEX, (_, index) => args[index]);
}
var PLACEHOLDER_MSG = getPlaceholder("msg");
var PLACEHOLDER_SVG = getPlaceholder("svg");
msg.html = function html(parts, ...args) {
	const input = getString(parts, args);
	return compile(input.replace(EXP_REGEX, (_, index) => getPlaceholder(index)), args, input + PLACEHOLDER_MSG, false, true);
};
msg.svg = function svg(parts, ...args) {
	const input = getString(parts, args);
	return compile(input.replace(EXP_REGEX, (_, index) => getPlaceholder(index)), args, input + PLACEHOLDER_MSG + PLACEHOLDER_SVG, true, true);
};
//#endregion
export { get, isLocalizeEnabled, localize, msg };
