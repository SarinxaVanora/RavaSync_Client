import { parse } from "../../../../tldts-experimental/dist/es6/index.js";
import logger_default from "./logger.js";
import { includesMiddleChar } from "./utils.js";
import { isHash } from "./hash-detector-v2.js";
//#region node_modules/@whotracksme/reporting/reporting/src/popularity-vote-sanitizer.js
/**
* WhoTracks.Me
* https://whotracks.me/
*
* Copyright 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0
*/
var MASKED = "#??#";
function sanitizeHostname(hostname) {
	const { domain } = parse(hostname, {
		extractHostname: false,
		mixedInputs: false,
		validateHostname: false
	});
	if (hostname === domain) return hostname;
	return hostname.split(".").map((part) => sanitizePart(part)).join(".");
}
function sanitizePathSegment(path) {
	return sanitizePart(path);
}
var MAX_TEXT_LENGTH = 128;
function ensureTextIsNotHuge(str) {
	if (str.length > MAX_TEXT_LENGTH) throw new Error(`Internal error: Long text should have been truncated before. Failed at text with ${str.length} > ${MAX_TEXT_LENGTH} characters: <<${str}>>`);
}
function sanitizePart(str) {
	if (str.includes(MASKED)) {
		logger_default.warn(`Found the reserved MASKED sequence (${MASKED}) in the input text: ${str}`);
		return MASKED;
	}
	const sanitized = sanitizePart__fixedButNotYetTruncated__(str);
	if (sanitized === str) return str;
	if (str.includes("%")) {
		let decodedStr;
		try {
			decodedStr = decodeURIComponent(str);
			if (decodedStr.includes(MASKED)) {
				logger_default.warn("Ignoring unlikely edge case where the reserved MASKED sequence was percent encoded:", str);
				decodedStr = void 0;
			} else if (hasInvalidSurrogates(decodedStr)) {
				logger_default.warn("Ignoring edge case where the decoded string contains invalid surrogates", str);
				decodedStr = void 0;
			}
		} catch {}
		if (decodedStr !== void 0 && decodedStr !== str) {
			const decodedSanitized = sanitizePart__fixedButNotYetTruncated__(decodedStr);
			if (decodedSanitized === decodedStr) return str;
			return backportMaskToOriginalText(str, decodedStr, decodedSanitized);
		}
	}
	return sanitized;
}
function sanitizePart__fixedButNotYetTruncated__(str) {
	if (str.length >= MAX_TEXT_LENGTH) {
		let splitAt = MAX_TEXT_LENGTH - 4;
		if (splitsSurrogatePair(str, splitAt)) splitAt -= 1;
		const truncated = str.slice(0, splitAt);
		logger_default.debug("Truncating long text segment:", str, "->", truncated);
		const sanitized = sanitizePart__fixedText__(truncated);
		return sanitized.endsWith(MASKED) ? sanitized : sanitized + MASKED;
	}
	return sanitizePart__fixedText__(str);
}
function sanitizePart__fixedText__(str) {
	ensureTextIsNotHuge(str);
	if (shouldBeMasked(str)) return MASKED;
	if (str.length >= 12 && includesMiddleChar(str, "+")) return MASKED;
	const { safePrefix, rest } = findSafePrefix(str);
	return mergeMaskInterruptions(safePrefix + maskHashes(rest));
}
function findSafePrefix(str, maxWords = 10) {
	if (maxWords <= 0) return {
		safePrefix: "",
		rest: str
	};
	let i = 0;
	while (i < str.length) {
		const code = str.charCodeAt(i);
		if (i > 0 && code === "-".charCodeAt(0)) {
			let { safePrefix: nextSafePrefix, rest } = findSafePrefix(str.slice(i + 1), maxWords - 1);
			if (shouldBeMasked(nextSafePrefix)) nextSafePrefix = MASKED;
			let thisSafePrefix = str.slice(0, i);
			if (shouldBeMasked(thisSafePrefix.endsWith("-") ? thisSafePrefix.slice(0, thisSafePrefix.length - 1) : thisSafePrefix)) thisSafePrefix = MASKED;
			return {
				safePrefix: `${thisSafePrefix}-${nextSafePrefix}`,
				rest
			};
		}
		if (isAsciiLowerCase(code) || isGreekLowerCase(code) || isArabic(code) || isGeorgian(code) || isCyrillicLowerCase(code) || isArmenianLowerCase(code)) i += 1;
		else break;
	}
	return {
		safePrefix: "",
		rest: str
	};
}
var MIN_SIZE_TO_BE_MASKED = 8;
function shouldBeMasked(str) {
	if (str.length < MIN_SIZE_TO_BE_MASKED) return false;
	if (str.length >= 8 && str.match(/^@?[\d,.]+z?$/)) return true;
	return !skipHashDetection(str) && isHash(str);
}
function maskHashes(str) {
	ensureTextIsNotHuge(str);
	for (let size = str.length; size >= MIN_SIZE_TO_BE_MASKED; size -= 1) for (let start = str.length - size; start >= 0; start -= 1) {
		if (splitsSurrogatePair(str, start) || splitsSurrogatePair(str, start + size)) continue;
		if (shouldBeMasked(str.slice(start, start + size))) {
			let prefix = "";
			if (start > 0) prefix = maskHashes(str.slice(0, start));
			let postfix = "";
			const beginPostfix = start + size;
			if (beginPostfix < str.length) postfix = maskHashes(str.slice(beginPostfix));
			const prefixMergable = prefix.endsWith(MASKED);
			const postfixMergable = postfix.startsWith(MASKED);
			if (prefixMergable && postfixMergable) return prefix + postfix.slice(4);
			if (prefixMergable || postfixMergable) return prefix + postfix;
			return prefix + MASKED + postfix;
		}
	}
	return str;
}
function mergeMaskInterruptions(str) {
	const lastMatch = str.lastIndexOf(MASKED);
	if (lastMatch === -1) return str;
	if (str === MASKED) return MASKED;
	const remainingCharsAfter = str.length - lastMatch - 4;
	if (remainingCharsAfter > 0 && remainingCharsAfter <= 8) return mergeMaskInterruptions(str.slice(0, str.length - remainingCharsAfter));
	const prevMatch = str.lastIndexOf(MASKED, lastMatch - 1);
	if (prevMatch === -1) return str;
	if (lastMatch - prevMatch - 4 <= 6) return mergeMaskInterruptions(str.slice(0, prevMatch + 4));
	return str;
}
function isAsciiLowerCase(code) {
	return code >= 97 && code <= 122;
}
function isGreekLowerCase(code) {
	return code >= 945 && code <= 969;
}
function isGeorgian(code) {
	return code >= 4304 && code <= 4351;
}
function isArabic(code) {
	return code >= 1536 && code <= 1791;
}
function isCyrillicLowerCase(code) {
	return code >= 1072 && code <= 1103 || code >= 1104 && code <= 1119;
}
function isArmenianLowerCase(code) {
	return code >= 1377 && code <= 1415;
}
function skipHashDetection(str) {
	const lowercaseWordDetectors = [
		{
			matches: isAsciiLowerCase,
			threshold: 6
		},
		{
			matches: isGreekLowerCase,
			threshold: 3
		},
		{
			matches: isArabic,
			threshold: 3
		},
		{
			matches: isGeorgian,
			threshold: 3
		},
		{
			matches: isCyrillicLowerCase,
			threshold: 4
		},
		{
			matches: isArmenianLowerCase,
			threshold: 3
		}
	];
	lowercaseWordDetectors.forEach((x) => x.current = 0);
	for (let i = 0; i < str.length; i += 1) {
		const code = str.charCodeAt(i);
		for (const detector of lowercaseWordDetectors) if (detector.matches(code)) {
			detector.current += 1;
			if (detector.current >= detector.threshold) return true;
		} else detector.current = 0;
	}
	return false;
}
function isHighSurrogate(code) {
	return (code & 64512) === 55296;
}
function isLowSurrogate(code) {
	return (code & 64512) === 56320;
}
function hasInvalidSurrogates(str) {
	for (let i = 0; i < str.length; i++) {
		const code = str.charCodeAt(i);
		if (isHighSurrogate(code)) {
			if (!isLowSurrogate(str.charCodeAt(i + 1))) return true;
			i++;
		} else if (isLowSurrogate(code)) return true;
	}
	return false;
}
function splitsSurrogatePair(str, pos) {
	if (pos <= 0 || pos >= str.length) return false;
	return isHighSurrogate(str.charCodeAt(pos - 1)) && isLowSurrogate(str.charCodeAt(pos));
}
function backportMaskToOriginalText(originalText, decodedText, maskedDecodedText) {
	if (maskedDecodedText.length === 0) return "";
	let nextMasked = maskedDecodedText.indexOf(MASKED);
	if (nextMasked === -1) return originalText;
	const matches = [];
	let posOrig = 0;
	let posDecoded = 0;
	let posMasked = 0;
	while (posMasked < maskedDecodedText.length) {
		const decodedPart = maskedDecodedText.slice(posMasked, nextMasked);
		const posDecodedMaskingBegins = posDecoded;
		posDecoded = decodedText.indexOf(decodedPart, posDecoded);
		if (posDecoded === -1) {
			logger_default.error("[backportMaskToOriginalText] Failed for example:", {
				originalText,
				decodedText,
				maskedDecodedText
			});
			throw new Error("Illegal state: expected a match.");
		}
		if (posDecoded > 0 && decodedText.indexOf(decodedPart, posDecoded + 1) !== -1) {
			const result = matches.join(MASKED) + MASKED;
			logger_default.warn("Conservative matching hit:", {
				originalText,
				decodedText,
				maskedDecodedText,
				result
			});
			return result;
		}
		const skipped = decodedText.slice(posDecodedMaskingBegins, posDecoded);
		posOrig += encodeURIComponent(skipped).length;
		const encodedPart = encodeURIComponent(decodedPart);
		matches.push(originalText.slice(posOrig, posOrig + encodedPart.length));
		posOrig += encodedPart.length;
		posDecoded += decodedPart.length;
		posMasked = nextMasked + 4;
		nextMasked = maskedDecodedText.indexOf(MASKED, posMasked);
		if (nextMasked === -1) {
			const fromEnd = encodeURIComponent(maskedDecodedText.slice(posMasked)).length;
			matches.push(originalText.slice(originalText.length - fromEnd));
			break;
		}
	}
	return matches.join(MASKED);
}
//#endregion
export { sanitizeHostname, sanitizePathSegment };
