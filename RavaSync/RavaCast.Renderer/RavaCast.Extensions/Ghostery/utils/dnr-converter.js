import convert$1 from "../npm/@ghostery/urlfilter2dnr/dist/esm/converters/adguard.js";
//#region src/utils/dnr-converter.js
/**
* Ghostery Browser Extension
* https://www.ghostery.com/
*
* Copyright 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0
*/
var DOCUMENT_PATH = "pages/dnr-converter/index.html";
async function convert(filters) {
	let result;
	try {
		if (chrome.offscreen) {
			await setupOffscreenDocument();
			result = await chrome.runtime.sendMessage({
				action: "dnr-converter:convert",
				filters
			});
		} else {
			result = await convert$1(filters, { resourcesPath: "/rule_resources/redirects" });
			result.errors = result.errors.map((e) => `DNR - ${e.message || e}`);
		}
	} catch (e) {
		return {
			errors: [e.message],
			rules: []
		};
	} finally {
		if (chrome.offscreen) closeOffscreenDocument();
	}
	for (const [index, rule] of result.rules.entries()) if (rule.condition.regexFilter) {
		const { isSupported, reason } = await chrome.declarativeNetRequest.isRegexSupported({ regex: rule.condition.regexFilter });
		if (!isSupported) {
			result.errors.push(`Could not apply a custom filter as "${rule.condition.regexFilter}" is a not supported regexp due to: ${reason}`);
			result.rules.splice(index, 1);
		}
	}
	return result;
}
var offscreenTimeout = null;
function closeOffscreenDocument() {
	if (offscreenTimeout) clearTimeout(offscreenTimeout);
	offscreenTimeout = setTimeout(() => {
		chrome.offscreen.closeDocument();
		offscreenTimeout = null;
	}, 1e3);
}
async function setupOffscreenDocument() {
	if (offscreenTimeout) clearTimeout(offscreenTimeout);
	const offscreenUrl = chrome.runtime.getURL(DOCUMENT_PATH);
	if (!(await chrome.runtime.getContexts({
		contextTypes: ["OFFSCREEN_DOCUMENT"],
		documentUrls: [offscreenUrl]
	})).length) await chrome.offscreen.createDocument({
		url: DOCUMENT_PATH,
		reasons: [chrome.offscreen.Reason.IFRAME_SCRIPTING],
		justification: "Convert network filters to DeclarativeNetRequest format."
	});
}
//#endregion
export { convert as default };
