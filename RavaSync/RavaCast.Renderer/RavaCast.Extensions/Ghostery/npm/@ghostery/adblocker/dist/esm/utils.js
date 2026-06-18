import { TOKENS_BUFFER } from "./tokens-buffer.js";
/*!
* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/
var HASH_SEED = 5011;
/***************************************************************************
*  Bitwise helpers
* ************************************************************************* */
function bitCount(n) {
	n = n - (n >> 1 & 1431655765);
	n = (n & 858993459) + (n >> 2 & 858993459);
	return (n + (n >> 4) & 252645135) * 16843009 >> 24;
}
function getBit(n, mask) {
	return !!(n & mask);
}
function setBit(n, mask) {
	return (n | mask) >>> 0;
}
function clearBit(n, mask) {
	return (n & ~mask) >>> 0;
}
function fastHashBetween(str, begin, end) {
	let hash = HASH_SEED;
	for (let i = begin; i < end; i += 1) hash = hash * 37 ^ str.charCodeAt(i);
	return hash >>> 0;
}
function fastHash(str) {
	if (typeof str !== "string") return HASH_SEED;
	if (str.length === 0) return HASH_SEED;
	return fastHashBetween(str, 0, str.length);
}
function hashStrings(strings) {
	const result = new Uint32Array(strings.length);
	let index = 0;
	for (const str of strings) result[index++] = fastHash(str);
	return result;
}
function isDigit(ch) {
	return ch >= 48 && ch <= 57;
}
function isAlpha(ch) {
	return ch >= 97 && ch <= 122 || ch >= 65 && ch <= 90;
}
function isAlphaExtended(ch) {
	return ch >= 192 && ch <= 450;
}
function isCyrillic(ch) {
	return ch >= 1024 && ch <= 1279;
}
function isAllowedCode(ch) {
	return isAlpha(ch) || isDigit(ch) || ch === 37 || isAlphaExtended(ch) || isCyrillic(ch);
}
function tokenizeWithWildcardsInPlace(pattern, skipFirstToken, skipLastToken, buffer) {
	const len = Math.min(pattern.length, buffer.remaining() * 2);
	let inside = false;
	let precedingCh = 0;
	let start = 0;
	let hash = HASH_SEED;
	for (let i = 0; i < len; i += 1) {
		const ch = pattern.charCodeAt(i);
		if (isAllowedCode(ch) === true) {
			if (inside === false) {
				hash = HASH_SEED;
				inside = true;
				start = i;
			}
			hash = hash * 37 ^ ch;
		} else {
			if (inside === true) {
				inside = false;
				if (i - start > 1 && ch !== 42 && precedingCh !== 42 && (skipFirstToken === false || start !== 0)) buffer.push(hash >>> 0);
			}
			precedingCh = ch;
		}
	}
	if (skipLastToken === false && inside === true && precedingCh !== 42 && pattern.length - start > 1 && buffer.full() === false) buffer.push(hash >>> 0);
}
function tokenizeInPlace(pattern, skipFirstToken, skipLastToken, buffer) {
	const len = Math.min(pattern.length, buffer.remaining() * 2);
	let inside = false;
	let start = 0;
	let hash = HASH_SEED;
	for (let i = 0; i < len; i += 1) {
		const ch = pattern.charCodeAt(i);
		if (isAllowedCode(ch) === true) {
			if (inside === false) {
				hash = HASH_SEED;
				inside = true;
				start = i;
			}
			hash = hash * 37 ^ ch;
		} else if (inside === true) {
			inside = false;
			if (i - start > 1 && (skipFirstToken === false || start !== 0)) buffer.push(hash >>> 0);
		}
	}
	if (inside === true && skipLastToken === false && pattern.length - start > 1 && buffer.full() === false) buffer.push(hash >>> 0);
}
function tokenizeNoSkipInPlace(pattern, buffer) {
	const len = Math.min(pattern.length, buffer.remaining() * 2);
	let inside = false;
	let start = 0;
	let hash = HASH_SEED;
	for (let i = 0; i < len; i += 1) {
		const ch = pattern.charCodeAt(i);
		if (isAllowedCode(ch) === true) {
			if (inside === false) {
				hash = HASH_SEED;
				inside = true;
				start = i;
			}
			hash = hash * 37 ^ ch;
		} else if (inside === true) {
			inside = false;
			if (i - start > 1) buffer.push(hash >>> 0);
		}
	}
	if (inside === true && pattern.length - start > 1 && buffer.full() === false) buffer.push(hash >>> 0);
}
function tokenizeNoSkip(pattern) {
	TOKENS_BUFFER.reset();
	tokenizeNoSkipInPlace(pattern, TOKENS_BUFFER);
	return TOKENS_BUFFER.slice();
}
function tokenize(pattern, skipFirstToken, skipLastToken) {
	TOKENS_BUFFER.reset();
	tokenizeInPlace(pattern, skipFirstToken, skipLastToken, TOKENS_BUFFER);
	return TOKENS_BUFFER.slice();
}
function tokenizeRegexInPlace(selector, tokens) {
	let end = selector.length - 1;
	let begin = 1;
	let prev = 0;
	for (; begin < end; begin += 1) {
		const code = selector.charCodeAt(begin);
		if (code === 124) return;
		if (code === 40 || code === 42 || code === 43 || code === 63 || code === 91 || code === 123 || code === 46 && prev !== 92 || code === 92 && isAlpha(selector.charCodeAt(begin + 1))) break;
		prev = code;
	}
	prev = 0;
	for (; end >= begin; end -= 1) {
		const code = selector.charCodeAt(end);
		if (code === 124) return;
		if (code === 41 || code === 42 || code === 43 || code === 63 || code === 93 || code === 125 || code === 46 && selector.charCodeAt(end - 1) !== 92 || code === 92 && isAlpha(prev)) break;
		prev = code;
	}
	if (end < begin) {
		const skipFirstToken = selector.charCodeAt(1) !== 94;
		const skipLastToken = selector.charCodeAt(selector.length - 1) !== 36;
		tokenizeInPlace(selector.slice(1, selector.length - 1), skipFirstToken, skipLastToken, tokens);
	} else {
		if (begin > 1) tokenizeInPlace(selector.slice(1, begin), selector.charCodeAt(1) !== 94, true, tokens);
		if (end < selector.length - 1) tokenizeInPlace(selector.slice(end + 1, selector.length - 1), true, selector.charCodeAt(selector.length - 1) !== 94, tokens);
	}
}
function binSearch(arr, elt) {
	if (arr.length === 0) return -1;
	let low = 0;
	let high = arr.length - 1;
	while (low <= high) {
		const mid = low + high >>> 1;
		const midVal = arr[mid];
		if (midVal < elt) low = mid + 1;
		else if (midVal > elt) high = mid - 1;
		else return mid;
	}
	return -1;
}
function binLookup(arr, elt) {
	return binSearch(arr, elt) !== -1;
}
var hasUnicodeRe = /[^\u0000-\u00ff]/;
function hasUnicode(str) {
	return hasUnicodeRe.test(str);
}
/**
* Finds the last index of an unescaped character in the given string.
* This function tries to find the match from the backward.
* When this function sees an escaping character, it will jump to the next index.
*/
function findLastIndexOfUnescapedCharacter(text, character) {
	let lastIndex = text.lastIndexOf(character);
	if (lastIndex === -1) return -1;
	while (lastIndex > 0 && text.charCodeAt(lastIndex - 1) === 92) lastIndex = text.lastIndexOf(character, lastIndex - 1);
	return lastIndex;
}
//#endregion
export { HASH_SEED, binLookup, bitCount, clearBit, fastHash, fastHashBetween, findLastIndexOfUnescapedCharacter, getBit, hasUnicode, hashStrings, isAlpha, isDigit, setBit, tokenize, tokenizeInPlace, tokenizeNoSkip, tokenizeNoSkipInPlace, tokenizeRegexInPlace, tokenizeWithWildcardsInPlace };
