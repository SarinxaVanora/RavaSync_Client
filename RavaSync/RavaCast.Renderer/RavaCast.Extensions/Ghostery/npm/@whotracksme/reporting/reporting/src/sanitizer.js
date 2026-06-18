import logger_default from "./logger.js";
import { isHash } from "./hash-detector.js";
//#region node_modules/@whotracksme/reporting/reporting/src/sanitizer.js
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
function isCharNumber(char) {
	const code = char.charCodeAt(0);
	return code >= 48 && code <= 57;
}
function uncheckedCharToNumber(char) {
	return char.charCodeAt(0) - 48;
}
function isValidEAN13(ean) {
	if (ean.length !== 13 || ![...ean].every(isCharNumber)) return false;
	let sum = 0;
	for (let i = 0; i < 12; i += 1) {
		const factor = i % 2 === 0 ? 1 : 3;
		sum += factor * uncheckedCharToNumber(ean[i]);
	}
	return 10 - sum % 10 === uncheckedCharToNumber(ean[12]);
}
function isValidISSN(issn) {
	if (!/^[0-9]{4}-?[0-9]{3}[0-9xX]$/.test(issn)) return false;
	issn = issn.replace("-", "");
	let checksum = 0;
	for (let i = 0; i < 7; i++) checksum += uncheckedCharToNumber(issn[i]) * (8 - i);
	const endsWithX = issn[7] === "x" || issn[7] === "X";
	checksum += endsWithX ? 10 : uncheckedCharToNumber(issn[7]);
	return checksum % 11 === 0;
}
/**
* Returns true if the given string contains any text that looks
* like an email address. The check is conservative, that means
* false positives are expected, but false negatives are not.
*/
function checkForEmail(str) {
	return /[a-z0-9\-_@]+(@|%40|%(25)+40)[a-z0-9\-_]+\.[a-z0-9\-_]/i.test(str);
}
/**
* Intended to filter out potentially problematic numbers.
* Tries to reduce the number of false-positives by detecting certain common
* product IDs (EAN, ISSN), which are common in search, but don't have personal
* information.
*
* Otherwise, it discard queries that contain numbers longer than 7 digits.
* So, 123456 is still allowed, but phone numbers like (090)90-2 or 5555 3235
* will be dropped.
*
* Note:
* - the current implementation discard anything that contains full dates
*   (e.g. "2023/05/17", "17.05.2023").
*  (TODO: perhaps this restriction should be reconsidered to allow a search
*   like "What happened on 24.12.1914?")
*/
function hasLongNumber(str) {
	const issn = str.split(" ").find(isValidISSN);
	if (issn) str = str.replace(issn, " ");
	const numbers = str.replace(/[^A-Za-z0-9]/g, "").replace(/[^0-9]+/g, " ").trim().split(" ").filter((num) => num.length > 2);
	if (numbers.length === 1) {
		const num = numbers[0];
		if (num.length === 13 && str.includes(num)) return !isValidEAN13(num);
	}
	return numbers.some((num) => num.length > 7);
}
function isLogogramChar(char) {
	const codePoint = char.codePointAt(0);
	if (codePoint >= 19968 && codePoint <= 40959) return true;
	if (codePoint >= 12352 && codePoint <= 12543) return true;
	if (codePoint >= 44032 && codePoint <= 55215) return true;
	if (codePoint >= 3584 && codePoint <= 3711) return true;
	return false;
}
/**
* Most languages have an alphabet where a word consist of multiple characters.
* But other languages (e.g. Chinese) use logograms, where a single character
* is equivalent to a word. Thus, heuristics need to be adjusted if they count
* the number of characters or words ("words" being defined as characters not
* separated by whitespace).
*
* Note: texts in Arabic or European languages should not trigger this check.
*/
function hasLogograms(str) {
	return [...str].some(isLogogramChar);
}
function checkSuspiciousQuery(query) {
	function accept() {
		return { accept: true };
	}
	function discard(reason) {
		return {
			accept: false,
			reason
		};
	}
	query = query.replace(/\s+/g, " ");
	if (query.length > 120) return discard("too long (120 character limit)");
	if (query.length > 50 && hasLogograms(query)) return discard("too long (50 characters and logograms are present)");
	const words = query.split(" ");
	if (words.length > 9) {
		if (words.filter((x) => x.length >= 4).length > 16) return discard("too many words");
		if (hasLogograms(query)) return discard("too many words (smaller limit but logograms are present");
	}
	if (hasLongNumber(query)) return discard("long number detected");
	if (checkForEmail(query)) return discard("looks like an email");
	if (/[^:]+:[^@]+@/.test(query)) return discard("looks like an http password");
	for (let i = 0; i < words.length; i += 1) {
		if (words[i].length > 45) return discard("found long word");
		if (words[i].length > 20 && !/^[a-zA-ZäöüéÄÖÜ][a-zäöüéß]+$/.test(words[i])) return discard("found long word (smaller limit but uncommon shape)");
	}
	return accept();
}
function tryParseUrl(url) {
	try {
		return new URL(url);
	} catch (e) {
		return null;
	}
}
function isPrivateHostname(hostname) {
	return hostname === "localhost" || hostname === "127.0.0.1";
}
function looksLikeIPv4Address(hostname) {
	return /^[0-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}$/.test(hostname);
}
function looksLikeSafeUrlParameter(key, value) {
	return value.length < 18 || /^[a-z-_]+$/.test(value);
}
/**
* There should be no reason for these URLs to show up, but if they do
* we should never send them to the backend. Especially, "moz-extension"
* is problematic, as it includes an id that is unique per user and
* can be used to link messages.
*/
function urlLeaksExtensionId(url) {
	return url.startsWith("moz-extension://") || url.startsWith("chrome-extension://");
}
function normalizeUrlPart(urlPart) {
	return urlPart.toLowerCase().replace(/_/g, "-");
}
var RISKY_URL_PATH_PARTS = new Set([
	"login",
	"login.php",
	"login-actions",
	"logout",
	"signin",
	"recover",
	"forgot",
	"forgot-password",
	"reset-credentials",
	"authenticate",
	"not-confirmed",
	"reset",
	"oauth",
	"password",
	"token",
	"edit",
	"checkout",
	"account",
	"share",
	"sharing",
	"admin",
	"console",
	"wp-admin",
	"wp-admin.php",
	"weblogic"
]);
/**
* Sanity checks to protect against accidentially sending sensitive URLs.
*
* There are three possible outcomes:
* 1) "safe": URL can be accepted as is
* 2) "truncated": URL may have sensitive parts but can be truncated
*    (use includ the hostname but remove the rest)
* 3) "dropped": URL is corrupted or unsafe
*
* Expections: this function should be seen as an additional layer of defence,
* but do not expect it to detect all situation. Instead, make sure to extract
* only URLs where the context is safe. Otherwise, you are expecting too
* much from this static classifier.
*
* When changing new rules here, it is OK to be conservative. Since
* classification error are expected, rather err on the side of
* dropping (or truncating) too much.
*
* Parameters:
* - strict [default=false]:
*     Enables additional checks that will truncate more ULRs, but will lead to
*     false-positives. Generally, the default of skipping these checks should
*     be best for most use cases; but in situations where the URL is not critical,
*     you can enable it for extra safety. Note that static filters will never be
*     perfect, but quorum should be used as an additional step; quorum is effective
*     in dropping unique identifiers like unique tokens that may slip through
*     static rules.
* - tryPreservePath [default=false]:
*     If the URL fails and gets truncated, it will normally keep only keep the
*     the scheme and the domain. With this option, it does an extra pass to
*     test if adding the URL path would be safe.
*     For instance, given "https://example.test/foo/bar?arg=1&...#hash..."),
*     it will check again with "https://example.test/foo/bar". If the latter URL
*     passes all (strict) checks, it will be used for the truncated URL; instead of
*     "https://example.test (PROTECTED)", the result will be
*     "https://example.test/foo/bar (PROTECTED)".
*/
function sanitizeUrl(url, options = {}) {
	const { strict = false, tryPreservePath = false } = options;
	let accept = () => ({
		result: "safe",
		safeUrl: url
	});
	const drop = (reason) => ({
		result: "dropped",
		safeUrl: null,
		reason
	});
	const parsedUrl = tryParseUrl(url);
	if (!parsedUrl) return drop("invalid URL");
	if (parsedUrl.username) return drop("URL sets username");
	if (parsedUrl.password) return drop("URL sets password");
	if (parsedUrl.port && parsedUrl.port !== "80" && parsedUrl.port !== "443") return drop("URL has uncommon port");
	if (parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") return drop("URL has uncommon protocol");
	if (isPrivateHostname(parsedUrl.hostname)) return drop("URL is not public");
	if (looksLikeIPv4Address(parsedUrl.hostname)) return drop("hostname is an ipv4 address");
	if (urlLeaksExtensionId(url)) return drop("URL leaks extension ID");
	try {
		const truncate = (reason) => {
			if (tryPreservePath && (parsedUrl.search || parsedUrl.hash)) {
				parsedUrl.search = "";
				parsedUrl.hash = "";
				const { result, safeUrl } = sanitizeUrl(parsedUrl.toString(), {
					...options,
					tryPreservePath: false
				});
				if (result === "safe") return {
					result: "truncated",
					safeUrl: `${safeUrl} (PROTECTED)`,
					reason
				};
			}
			const safeUrl = `${parsedUrl.protocol}//${parsedUrl.hostname}/ (PROTECTED)`;
			logger_default.debug("sanitizeUrl truncated URL:", url, "->", safeUrl);
			return {
				result: "truncated",
				safeUrl,
				reason
			};
		};
		if (parsedUrl.hostname.length > 50) return drop("hostname too long");
		if (url.length > 800) return truncate("url too long");
		if (parsedUrl.search.length > 150) return truncate("url search part too long");
		if (parsedUrl.searchParams.size > 8) return truncate("too many url search parameters");
		const decodedUrl = decodeURIComponent(url);
		if (checkForEmail(url) || checkForEmail(decodedUrl)) return truncate("potential email found");
		const pathParts = parsedUrl.pathname.split("/");
		if (pathParts.length > 8) return truncate("too many parts in the url path");
		for (const part of pathParts) {
			const normalizedPart = normalizeUrlPart(part);
			if (RISKY_URL_PATH_PARTS.has(normalizedPart)) return truncate(`Found a problematic part in the URL path: ${part}`);
			if (strict && isHash(part, { threshold: .015 })) return truncate(`Found URL path that could be an identifier: <<${part}>>`);
		}
		for (const regexp of [
			/[&?]redirect(?:-?url)?=/i,
			/[&?#/=;](?:http|https)(?::[/]|%3A%2F)/,
			/[/]order[/]./i,
			/[/]auth[/]realms[/]/i,
			/[/]protocol[/]openid-connect[/]/i,
			/((maps|route[^r-]).*|@)\d{1,2}[^\d]-?\d{6}.+\d{1,2}[^\d]-?\d{6}/i
		]) if (regexp.test(url)) return truncate(`matches ${regexp}`);
		for (const [key, value] of parsedUrl.searchParams) {
			if (value.length > 18 && !looksLikeSafeUrlParameter(key, value)) {
				const { accept: ok, reason } = checkSuspiciousQuery(value);
				if (!ok) return truncate(`Found problematic URL parameter ${key}=${value}: ${reason}`);
			}
			if (strict && isHash(value, { threshold: .015 })) return truncate(`Found URL parameter that could be an identifier ${key}=${value}`);
		}
		if (parsedUrl.hash) {
			parsedUrl.hash = "";
			const safeUrl = `${parsedUrl} (PROTECTED)`;
			logger_default.debug("sanitizeUrl truncated URL:", url, "->", safeUrl);
			return {
				result: "truncated",
				safeUrl,
				reason: "URL fragment found"
			};
		}
		return accept();
	} catch (e) {
		logger_default.warn(`Unexpected error in sanitizeUrl. Skipping url=${url}`, e);
		return drop("Unexpected error");
	}
}
/**
* Set of heuristics to prevent accidentally leaking sensitive data.
*
* It is a hard problem to classify sensititive from non-sensitive data.
* If you look at the risk of false positives (non-sensitive data being dropped)
* versus fast negatives (sensitive data being sent out), the risks are very clear.
*
* Leaking sensitive data is far more dangerous then dropping harmless messages
* for multiple reasons. Because of that, we should always thrive for being
* rather too conservative than being to open when it comes to the heuristics.
*
* In other words, because of the non-symmetric risks, it is unavoidable that
* you will find many harmless examples that will be rejected by the rules.
*/
var Sanitizer = class {
	constructor(countryProvider) {
		this.countryProvider = countryProvider;
	}
	checkSuspiciousQuery(query) {
		const result = checkSuspiciousQuery(query);
		if (!result.accept) logger_default.debug("checkSuspiciousQuery rejected query:", query, ", reason:", result.reason);
		return result;
	}
	getSafeCountryCode() {
		return this.countryProvider.getSafeCountryCode();
	}
};
//#endregion
export { Sanitizer as default, sanitizeUrl };
