import { getFunctionName, getVueInternalName } from "./stacktrace.js";
import { isSyntheticEvent, isVueViewModel } from "./is.js";
import { convertToPlainObject } from "./object.js";
import { getNormalizationDepthOverrideHint, hasSkipNormalizationHint } from "./normalizationHints.js";
//#region node_modules/@sentry/core/build/esm/utils/normalize.js
/**
* Recursively normalizes the given object.
*
* - Creates a copy to prevent original input mutation
* - Skips non-enumerable properties
* - When stringifying, calls `toJSON` if implemented
* - Removes circular references
* - Translates non-serializable values (`undefined`/`NaN`/functions) to serializable format
* - Translates known global objects/classes to a string representations
* - Takes care of `Error` object serialization
* - Optionally limits depth of final output
* - Optionally limits number of properties/elements included in any single object/array
*
* @param input The object to be normalized.
* @param depth The max depth to which to normalize the object. (Anything deeper stringified whole.)
* @param maxProperties The max number of elements or properties to be included in any single array or
* object in the normalized output.
* @returns A normalized version of the object, or `"**non-serializable**"` if any errors are thrown during normalization.
*/
function normalize(input, depth = 100, maxProperties = Infinity) {
	try {
		return visit("", input, depth, maxProperties);
	} catch (err) {
		return { ERROR: `**non-serializable** (${err})` };
	}
}
/** JSDoc */
function normalizeToSize(object, depth = 3, maxSize = 100 * 1024) {
	const normalized = normalize(object, depth);
	if (jsonSize(normalized) > maxSize) return normalizeToSize(object, depth - 1, maxSize);
	return normalized;
}
/**
* Visits a node to perform normalization on it
*
* @param key The key corresponding to the given node
* @param value The node to be visited
* @param depth Optional number indicating the maximum recursion depth
* @param maxProperties Optional maximum number of properties/elements included in any single object/array
* @param memo Optional Memo class handling decycling
*/
function visit(key, value, depth = Infinity, maxProperties = Infinity, memo = memoBuilder()) {
	const [memoize, unmemoize] = memo;
	if (value == null || ["boolean", "string"].includes(typeof value) || typeof value === "number" && Number.isFinite(value)) return value;
	const stringified = stringifyValue(key, value);
	if (!stringified.startsWith("[object ")) return stringified;
	if (hasSkipNormalizationHint(value)) return value;
	const overrideDepth = getNormalizationDepthOverrideHint(value);
	const remainingDepth = overrideDepth !== void 0 ? overrideDepth : depth;
	if (remainingDepth === 0) return stringified.replace("object ", "");
	if (memoize(value)) return "[Circular ~]";
	const valueWithToJSON = value;
	if (valueWithToJSON && typeof valueWithToJSON.toJSON === "function") try {
		return visit("", valueWithToJSON.toJSON(), remainingDepth - 1, maxProperties, memo);
	} catch {}
	const normalized = Array.isArray(value) ? [] : {};
	let numAdded = 0;
	const visitable = convertToPlainObject(value);
	for (const visitKey in visitable) {
		if (!Object.prototype.hasOwnProperty.call(visitable, visitKey)) continue;
		if (numAdded >= maxProperties) {
			normalized[visitKey] = "[MaxProperties ~]";
			break;
		}
		const visitValue = visitable[visitKey];
		normalized[visitKey] = visit(visitKey, visitValue, remainingDepth - 1, maxProperties, memo);
		numAdded++;
	}
	unmemoize(value);
	return normalized;
}
/**
* Stringify the given value. Handles various known special values and types.
*
* Not meant to be used on simple primitives which already have a string representation, as it will, for example, turn
* the number 1231 into "[Object Number]", nor on `null`, as it will throw.
*
* @param value The value to stringify
* @returns A stringified representation of the given value
*/
function stringifyValue(key, value) {
	try {
		if (key === "domain" && value && typeof value === "object" && value._events) return "[Domain]";
		if (key === "domainEmitter") return "[DomainEmitter]";
		if (typeof global !== "undefined" && value === global) return "[Global]";
		if (typeof window !== "undefined" && value === window) return "[Window]";
		if (typeof document !== "undefined" && value === document) return "[Document]";
		if (isVueViewModel(value)) return getVueInternalName(value);
		if (isSyntheticEvent(value)) return "[SyntheticEvent]";
		if (typeof value === "number" && !Number.isFinite(value)) return `[${value}]`;
		if (typeof value === "function") return `[Function: ${getFunctionName(value)}]`;
		if (typeof value === "symbol") return `[${String(value)}]`;
		if (typeof value === "bigint") return `[BigInt: ${String(value)}]`;
		const objName = getConstructorName(value);
		if (/^HTML(\w*)Element$/.test(objName)) return `[HTMLElement: ${objName}]`;
		return `[object ${objName}]`;
	} catch (err) {
		return `**non-serializable** (${err})`;
	}
}
function getConstructorName(value) {
	const prototype = Object.getPrototypeOf(value);
	return prototype?.constructor ? prototype.constructor.name : "null prototype";
}
/** Calculates bytes size of input string */
function utf8Length(value) {
	return ~-encodeURI(value).split(/%..|./).length;
}
/** Calculates bytes size of input object */
function jsonSize(value) {
	return utf8Length(JSON.stringify(value));
}
/**
* Helper to decycle json objects
*/
function memoBuilder() {
	const inner = /* @__PURE__ */ new WeakSet();
	function memoize(obj) {
		if (inner.has(obj)) return true;
		inner.add(obj);
		return false;
	}
	function unmemoize(obj) {
		inner.delete(obj);
	}
	return [memoize, unmemoize];
}
//#endregion
export { normalize, normalizeToSize };
