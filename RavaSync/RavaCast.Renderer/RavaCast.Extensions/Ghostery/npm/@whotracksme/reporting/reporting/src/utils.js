import "./logger.js";
import cyrb53 from "./cyrb53.js";
import SeqExecutor from "./seq-executor.js";
//#region node_modules/@whotracksme/reporting/reporting/src/utils.js
function isNil(x) {
	return x === void 0 || x === null;
}
function requireParam(x, name) {
	if (isNil(x)) throw new Error(name ? `Required parameter "${name}" is missing` : "Required parameter is missing");
	return x;
}
function requireInt(value, name) {
	if (!Number.isInteger(value)) throw new Error(name ? `${name} should be integer, but got: <${value}>` : `Parameter should be integer, but got: <${value}>`);
	return value;
}
function requireIntOrNull(value, name) {
	if (value !== null && !Number.isInteger(value)) throw new Error(name ? `${name} should be integer or null, but got: <${value}>` : `Parameter should be integer or null, but got: <${value}>`);
	return value;
}
function requireUTC(value, name) {
	if (!Number.isSafeInteger(value) || value < 0) throw new Error(name ? `${name} should be a UTC timestamp, but got: <${value}>` : `Parameter should be a UTC timestamp, but got: <${value}>`);
	return value;
}
function requireString(value, name) {
	if (typeof value !== "string") throw new Error(name ? `${name} should be string, but got: <${value}>` : `Parameter should be string, but got: <${value}>`);
	return value;
}
function requireStringOrNull(value, name) {
	if (value !== null && typeof value !== "string") throw new Error(name ? `${name} should be string or null, but got: <${value}>` : `Parameter should be string or null, but got: <${value}>`);
	return value;
}
function requireArrayOfStrings(value, name) {
	if (!Array.isArray(value)) throw new Error(name ? `${name} should be an array of strings, but got: ${value}` : `Parameter should be an array of strings, but got: ${value}`);
	value.forEach((x, idx) => {
		if (typeof x !== "string") throw new Error(name ? `${name} should be an array of string, but got: ${value} (stopped at pos #${idx}: ${x})` : `Parameter should be an array of string, but got: ${value} (stopped at pos #${idx}: ${x})`);
	});
	return value;
}
function requireBoolean(value, name) {
	if (value !== true && value !== false) throw new Error(name ? `${name} should be boolean, but got: ${value}` : `Parameter should be boolean, but got: ${value}`);
	return value;
}
/**
* This is a narrow definition of object (i.e. something like {}).
*/
function requireObject(value, name) {
	if (isNil(value) || Array.isArray(value) || typeof value !== "object") throw new Error(name ? `${name} should be an object, but got: <${value}>` : `Parameter should be an object, but got: <${value}>`);
	return value;
}
/**
* If you need cryptographically safe randomness, consider using
* randomBetween from './random.js'.
*/
function randBetween(min, max) {
	return min + Math.random() * (max - min);
}
function clamp({ min, max, value }) {
	return Math.min(Math.max(min, value), max);
}
function intersectMapKeys(map1, map2) {
	const small = map1.size <= map2.size ? map1 : map2;
	const big = map1.size <= map2.size ? map2 : map1;
	return [...small.keys()].filter((x) => big.has(x));
}
/**
* A non-cryptographic hash function for strings.
*
* Limitations:
* - Expect the output space to be (at most) 53 bits.
* - Expect the implementation to change at any point, and use it only
*   for hashes that are stored locally on the profile.
*
* Options:
* - seed: overwrites the seed (may be ignored)
* - truncate: weakens the hash to 32 bits to make collisions more likely
* - output: 'string', 'number', but by default unspecified
*
* If you require the output to string or number, you should set the
* output parameter. Otherwise, leave it empty, so that the implementation
* can chose (see USE_CLEARTEXT_HASHES_IF_POSSIBLE).
*/
function fastHash(str, { seed = 0, truncate = false, output } = {}) {
	let hash = cyrb53(str, seed);
	if (truncate) hash = (hash & 4294967295) >>> 0;
	if (output === "string") return hash.toString(36);
	return hash;
}
/**
* Example: chunk([1, 2, 3, 4, 5], 3) ==> [[1, 2, 3], [4, 5]]
*/
function chunk(array, size) {
	if (size <= 0) throw new Error(`Batch size must be strictly positive, but got ${size}`);
	const result = [];
	for (let i = 0; i < array.length; i += size) result.push(array.slice(i, i + Math.min(array.length - i, size)));
	return result;
}
/**
* Example: flattenObject({ x: 1, y: { z: 2 } }) ==> [{ path: ['x'], value: 1 }, { path: ['y', 'z'], value: 2 }]
*/
function flattenObject(obj, parentPath = []) {
	return Object.entries(obj).reduce((acc, [key, value]) => {
		const path = [...parentPath, key];
		if (typeof value === "object" && value !== null) acc.push(...flattenObject(value, path));
		else acc.push({
			path,
			value
		});
		return acc;
	}, []);
}
function sortedObjectEntries(x) {
	return Object.entries(x).sort((a, b) => {
		if (a[0] < b[0]) return -1;
		if (a[0] > b[0]) return 1;
		return 0;
	});
}
/**
* This is not a full deepEqual implementation. But it should be
* able to detect common data objects with primitive types.
*
* If it returns true, you may assume that both inputs represent
* the same data.

* WARN: This implementation will not detect identity in all situations;
* you may still get false for identical objects.
* If you need stronger guarantees, use a real deepEqual implementation.
*
* Note: the implementation assumes that objects are free of cycles
*/
function equalityCanBeProven(x, y) {
	if (x === true || x === false || x === null || x === void 0 || typeof x === "string" || Number.isFinite(x)) return x === y;
	if (x.constructor === Date) return y?.constructor === Date && +x === +y;
	if (Array.isArray(x)) {
		if (!Array.isArray(y) || x.length !== y.length) return false;
		for (let i = 0; i < x.length; i += 1) if (!equalityCanBeProven(x[i], y[i])) return false;
		return true;
	}
	if (x.constructor === Object) {
		if (y?.constructor !== Object) return false;
		return y.constructor === Object && equalityCanBeProven(sortedObjectEntries(x), sortedObjectEntries(y));
	}
	return false;
}
function roundUpToNextUTCMidnight(unixEpoch) {
	const date = new Date(unixEpoch);
	date.setUTCHours(24, 0, 0, 0);
	return date.getTime();
}
/**
* split0(str, on) === str.split(on)[0]
*/
function split0(str, on) {
	const pos = str.indexOf(on);
	return pos < 0 ? str : str.slice(0, pos);
}
/**
* Checks if a string contains a specified character at least once,
* excluding any leading or trailing occurrences of that character.
*
* Example:
* 1) includesMiddleChar('a+b', '+') ==> true
* 2) includesMiddleChar('+ab', '+') ==> false
* 3) includesMiddleChar('ab+', '+') ==> false
*/
function includesMiddleChar(str, char) {
	let start = 0;
	let end = str.length - 1;
	while (start < end && str[start] === char) start += 1;
	while (end > start && str[end] === char) end -= 1;
	for (let i = start; i < end; i += 1) if (str[i] === char) return true;
	return false;
}
/**
* Drop-in replacement for JSON.parse if you need to handle untrusted data.
*
* Warning: this implementation will not be able to magically solve all
* possible attacks. Depending on the specific use case, additional
* verifications may be needed (e.g. allowing only specific known fields).
*/
function parseUntrustedJSON(untrustedJson, { sanitizeSilently = false, maxSize } = {}) {
	if (typeof untrustedJson !== "string") throw new Error("Untrusted JSON detected: unexpected input to be of type \"string\"");
	if (!isNil(maxSize) && untrustedJson.length > maxSize) throw new Error(`Untrusted JSON detected: input exceeded maximum size of ${maxSize}`);
	if (sanitizeSilently) {
		const filterBadKeys = (key, value) => key === "__proto__" ? void 0 : value;
		return JSON.parse(untrustedJson, filterBadKeys);
	}
	const failOnBadKeys = (key, value) => {
		if (key === "__proto__") throw new Error("Untrusted JSON detected: unexpected __proto__");
		return value;
	};
	return JSON.parse(untrustedJson, failOnBadKeys);
}
/**
* Lazy initialized variables:
* - evaluated once at the first request
* - never evaluated more than once
* - never evaluated unless requested
*
* Usage:
* const fooProvider = lazyInitAsync(async () => 1 + 1);
* const foo = await fooProvider(); // foo === 2
*
* Warning: avoid cyclic dependencies in the initialization; otherwise,
* you risk deadlocks. The implementation will not attempt to detect it.
*/
function lazyInitAsync(func) {
	const criticalSection = new SeqExecutor();
	let isFirstCall = true;
	let initFailed = false;
	let value;
	let error;
	return () => {
		return criticalSection.run(async () => {
			if (isFirstCall) {
				try {
					value = await func();
				} catch (e) {
					initFailed = true;
					error = e;
				}
				isFirstCall = false;
			}
			if (initFailed) throw error;
			return value;
		});
	};
}
//#endregion
export { chunk, clamp, equalityCanBeProven, fastHash, flattenObject, includesMiddleChar, intersectMapKeys, isNil, lazyInitAsync, parseUntrustedJSON, randBetween, requireArrayOfStrings, requireBoolean, requireInt, requireIntOrNull, requireObject, requireParam, requireString, requireStringOrNull, requireUTC, roundUpToNextUTCMidnight, split0 };
