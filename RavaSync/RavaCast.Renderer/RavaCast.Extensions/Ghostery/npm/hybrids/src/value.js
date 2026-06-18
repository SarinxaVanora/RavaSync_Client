import { camelToDash } from "./utils.js";
//#region node_modules/hybrids/src/value.js
function reflect(host, value, attrName) {
	if (!value && value !== 0) host.removeAttribute(attrName);
	else host.setAttribute(attrName, value === true ? "" : value);
}
function value(key, desc) {
	const type = typeof desc.value;
	const defaultValue = type === "object" ? Object.freeze(desc.value) : desc.value;
	switch (type) {
		case "string":
			desc.value = (host, value) => value !== void 0 ? String(value) : defaultValue;
			break;
		case "number":
			desc.value = (host, value) => value !== void 0 ? Number(value) : defaultValue;
			break;
		case "boolean":
			desc.value = (host, value) => value !== void 0 ? Boolean(value) : defaultValue;
			break;
		case "function":
			desc.value = defaultValue;
			break;
		default: desc.value = (_, value = defaultValue) => value;
	}
	let observe = desc.observe;
	if (desc.reflect) {
		const attrName = camelToDash(key);
		const fn = typeof desc.reflect === "function" ? (host, value, attrName) => reflect(host, desc.reflect(value), attrName) : reflect;
		observe = desc.observe ? (host, value, lastValue) => {
			fn(host, value, attrName);
			desc.observe(host, value, lastValue);
		} : (host, value) => fn(host, value, attrName);
	}
	return {
		...desc,
		observe,
		writable: type !== "function" || defaultValue.length > 1
	};
}
//#endregion
export { value as default };
