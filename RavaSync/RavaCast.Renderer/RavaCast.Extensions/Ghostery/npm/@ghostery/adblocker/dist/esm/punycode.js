//#region node_modules/@ghostery/adblocker/dist/esm/punycode.js
/*!
* Copyright Mathias Bynens <https://mathiasbynens.be/>
*
* Permission is hereby granted, free of charge, to any person obtaining
* a copy of this software and associated documentation files (the
* "Software"), to deal in the Software without restriction, including
* without limitation the rights to use, copy, modify, merge, publish,
* distribute, sublicense, and/or sell copies of the Software, and to
* permit persons to whom the Software is furnished to do so, subject to
* the following conditions:
*
* The above copyright notice and this permission notice shall be
* included in all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
* MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
* NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
* LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
* OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
* WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
/** Highest positive signed 32-bit float value */
var maxInt = 2147483647;
/** Bootstring parameters */
var base = 36;
var tMin = 1;
var tMax = 26;
var skew = 38;
var damp = 700;
var initialBias = 72;
var initialN = 128;
var delimiter = "-";
/** Regular expressions */
var regexNonASCII = /[^\0-\x7E]/;
var regexSeparators = /[\x2E\u3002\uFF0E\uFF61]/g;
var errors = {
	"invalid-input": "Invalid input",
	"not-basic": "Illegal input >= 0x80 (not a basic code point)",
	"overflow": "Overflow: input needs wider integers to process"
};
/** Convenience shortcuts */
var baseMinusTMin = base - tMin;
/**
* A generic error utility function.
* @private
* @param {String} type The error type.
* @returns {Error} Throws a `RangeError` with the applicable error message.
*/
function error(type) {
	throw new RangeError(errors[type]);
}
/**
* Creates an array containing the numeric code points of each Unicode
* character in the string. While JavaScript uses UCS-2 internally,
* this function will convert a pair of surrogate halves (each of which
* UCS-2 exposes as separate characters) into a single code point,
* matching UTF-16.
* @see `punycode.ucs2.encode`
* @see <https://mathiasbynens.be/notes/javascript-encoding>
* @memberOf punycode.ucs2
* @name decode
* @param {String} string The Unicode input string (UCS-2).
* @returns {Array} The new array of code points.
*/
function ucs2decode(str) {
	const output = [];
	let counter = 0;
	const length = str.length;
	while (counter < length) {
		const value = str.charCodeAt(counter++);
		if (value >= 55296 && value <= 56319 && counter < length) {
			const extra = str.charCodeAt(counter++);
			if ((extra & 64512) === 56320) output.push(((value & 1023) << 10) + (extra & 1023) + 65536);
			else {
				output.push(value);
				counter--;
			}
		} else output.push(value);
	}
	return output;
}
/**
* Converts a digit/integer into a basic code point.
* @see `basicToDigit()`
* @private
* @param {Number} digit The numeric value of a basic code point.
* @returns {Number} The basic code point whose value (when used for
* representing integers) is `digit`, which needs to be in the range
* `0` to `base - 1`. If `flag` is non-zero, the uppercase form is
* used; else, the lowercase form is used. The behavior is undefined
* if `flag` is non-zero and `digit` has no uppercase form.
*/
function digitToBasic(digit, flag) {
	return digit + 22 + 75 * (digit < 26 ? 1 : 0) - ((flag !== 0 ? 1 : 0) << 5);
}
/**
* Bias adaptation function as per section 3.4 of RFC 3492.
* https://tools.ietf.org/html/rfc3492#section-3.4
* @private
*/
function adapt(delta, numPoints, firstTime) {
	let k = 0;
	delta = firstTime ? Math.floor(delta / damp) : delta >> 1;
	delta += Math.floor(delta / numPoints);
	for (; delta > 455; k += base) delta = Math.floor(delta / baseMinusTMin);
	return Math.floor(k + 36 * delta / (delta + skew));
}
/**
* Converts a string of Unicode symbols (e.g. a domain name label) to a
* Punycode string of ASCII-only symbols.
* @memberOf punycode
* @param {String} input The string of Unicode symbols.
* @returns {String} The resulting Punycode string of ASCII-only symbols.
*/
function encode(str) {
	const output = [];
	const input = ucs2decode(str);
	const inputLength = input.length;
	let n = initialN;
	let delta = 0;
	let bias = initialBias;
	for (let i = 0; i < input.length; i += 1) {
		const currentValue = input[i];
		if (currentValue < 128) output.push(String.fromCharCode(currentValue));
	}
	const basicLength = output.length;
	let handledCPCount = basicLength;
	if (basicLength) output.push(delimiter);
	while (handledCPCount < inputLength) {
		let m = maxInt;
		for (let i = 0; i < input.length; i += 1) {
			const currentValue = input[i];
			if (currentValue >= n && currentValue < m) m = currentValue;
		}
		const handledCPCountPlusOne = handledCPCount + 1;
		if (m - n > Math.floor((maxInt - delta) / handledCPCountPlusOne)) error("overflow");
		delta += (m - n) * handledCPCountPlusOne;
		n = m;
		for (let i = 0; i < input.length; i += 1) {
			const currentValue = input[i];
			if (currentValue < n && ++delta > maxInt) error("overflow");
			if (currentValue === n) {
				let q = delta;
				for (let k = base;; k += base) {
					const t = k <= bias ? tMin : k >= bias + tMax ? tMax : k - bias;
					if (q < t) break;
					const qMinusT = q - t;
					const baseMinusT = base - t;
					output.push(String.fromCharCode(digitToBasic(t + qMinusT % baseMinusT, 0)));
					q = Math.floor(qMinusT / baseMinusT);
				}
				output.push(String.fromCharCode(digitToBasic(q, 0)));
				bias = adapt(delta, handledCPCountPlusOne, handledCPCount === basicLength);
				delta = 0;
				++handledCPCount;
			}
		}
		++delta;
		++n;
	}
	return output.join("");
}
/**
* Converts a Unicode string representing a domain name or an email address to
* Punycode. Only the non-ASCII parts of the domain name will be converted,
* i.e. it doesn't matter if you call it with a domain that's already in
* ASCII.
* @memberOf punycode
* @param {String} input The domain name or email address to convert, as a
* Unicode string.
* @returns {String} The Punycode representation of the given domain name or
* email address.
*/
function toASCII(input) {
	const labels = input.replace(regexSeparators, ".").split(".");
	const encoded = [];
	for (let i = 0; i < labels.length; i += 1) encoded.push(regexNonASCII.test(labels[i]) ? "xn--" + encode(labels[i]) : labels[i]);
	return encoded.join(".");
}
//#endregion
export { toASCII };
