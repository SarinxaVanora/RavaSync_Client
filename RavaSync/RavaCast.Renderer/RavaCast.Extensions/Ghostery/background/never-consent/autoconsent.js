import store_default from "../../npm/hybrids/src/store.js";
import Options, { getPausedDetails } from "../../store/options.js";
import "../../npm/@ghostery/config/dist/esm/actions.js";
import Config from "../../store/config.js";
import Resources from "../../store/resources.js";
import { parseWithCache } from "../../utils/request.js";
import { filterCompactRules, snippets } from "../../npm/@duckduckgo/autoconsent/dist/autoconsent.esm.js";
import compact_rules_default from "../../npm/@duckduckgo/autoconsent/rules/compact-rules.json.js";
//#region src/background/never-consent/autoconsent.js
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
async function initialize(msg, sender) {
	const [options, config] = await Promise.all([store_default.resolve(Options), store_default.resolve(Config)]);
	if (options.terms && options.blockAnnoyances) {
		const { tab, frameId } = sender;
		const senderUrl = sender.url || `${sender.origin}/`;
		const hostname = senderUrl ? parseWithCache(senderUrl).hostname : "";
		if (getPausedDetails(options, hostname) || config.hasAction(hostname, "disable-autoconsent")) return;
		const compact = filterCompactRules(compact_rules_default, {
			url: senderUrl,
			mainFrame: frameId === 0
		});
		try {
			chrome.tabs.sendMessage(tab.id, {
				action: "autoconsent",
				type: "initResp",
				rules: { compact },
				config: {
					autoAction: options.autoconsent.autoAction,
					enableCosmeticRules: false,
					enableFilterList: false
				}
			}, { frameId });
		} catch {}
	}
}
async function evalCode(snippetId, id, tabId, frameId) {
	const [result] = await chrome.scripting.executeScript({
		target: {
			tabId,
			frameIds: [frameId]
		},
		world: chrome.scripting.ExecutionWorld?.MAIN ?? "MAIN",
		func: snippets[snippetId]
	});
	await chrome.tabs.sendMessage(tabId, {
		action: "autoconsent",
		id,
		type: "evalResp",
		result: result.result
	}, { frameId });
}
chrome.runtime.onMessage.addListener((msg, sender) => {
	if (msg.action !== "autoconsent") return;
	if (!sender.tab) return;
	const frameId = sender.frameId;
	switch (msg.type) {
		case "init": return initialize(msg, sender);
		case "eval": return evalCode(msg.snippetId, msg.id, sender.tab.id, frameId);
		case "optInResult":
		case "optOutResult":
			if (msg.result === true) {
				const { domain } = parseWithCache(sender.url);
				if (domain) store_default.set(Resources, { autoconsent: { [domain]: Date.now() } });
			}
			break;
		default: break;
	}
});
//#endregion
