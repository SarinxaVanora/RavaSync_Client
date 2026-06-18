import store_default from "../../npm/hybrids/src/store.js";
import Options, { getPausedDetails } from "../../store/options.js";
import "../../utils/options-observer.js";
import { FLAG_SUBFRAME_SCRIPTING } from "../../npm/@ghostery/config/dist/esm/flags.js";
import { resolveFlag } from "../../store/config.js";
import { MAIN_ENGINE, get } from "../../utils/engines.js";
import { parseWithCache } from "../../utils/request.js";
import scriptlets from "../../npm/@ghostery/scriptlets/index.js";
import "./content-scripts.js";
import { setup } from "./engines.js";
import { tabStats } from "../stats.js";
import { FramesHierarchy } from "./ancestors.js";
//#region src/background/adblocker/cosmetics.js
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
function resolveInjectionTarget(details) {
	const target = { tabId: details.tabId };
	if (details.documentId) target.documentIds = [details.documentId];
	else target.frameIds = [details.frameId];
	return target;
}
var scriptletGlobals = { warOrigin: chrome.runtime.getURL("/rule_resources/redirects/empty").slice(0, -6) };
function injectScriptlets(filters, hostname, details) {
	for (const filter of filters) {
		const parsed = filter.parseScript();
		if (!parsed) {
			console.warn("[adblocker] could not inject script filter:", filter.toString());
			continue;
		}
		const scriptletName = `${parsed.name}${parsed.name.endsWith(".js") ? "" : ".js"}`;
		const scriptlet = scriptlets[scriptletName];
		if (!scriptlet) {
			console.warn("[adblocker] unknown scriptlet with name:", scriptletName);
			continue;
		}
		const func = scriptlet.func;
		const args = [scriptletGlobals, ...parsed.args.map((arg) => decodeURIComponent(arg))];
		chrome.scripting.executeScript({
			injectImmediately: true,
			world: chrome.scripting.ExecutionWorld?.MAIN ?? "MAIN",
			target: resolveInjectionTarget(details),
			func,
			args
		}, () => {
			if (chrome.runtime.lastError) console.warn(chrome.runtime.lastError);
		});
	}
}
function injectStyles(styles, details) {
	chrome.scripting.insertCSS({
		css: styles,
		origin: "USER",
		target: resolveInjectionTarget(details)
	}).catch(async (e) => {
		const frame = await chrome.webNavigation.getFrame({
			tabId: details.tabId,
			frameId: details.frameId
		}).catch(() => null);
		if (!frame) return;
		if (details.documentId && frame.documentId !== details.documentId) return;
		console.warn("[adblocker] failed to inject CSS", e);
	});
}
var SUBFRAME_SCRIPTING = resolveFlag(FLAG_SUBFRAME_SCRIPTING);
var framesHierarchy = new FramesHierarchy();
framesHierarchy.handleWebWorkerStart();
framesHierarchy.handleWebextensionEvents();
async function injectCosmetics(details, config) {
	const { bootstrap: isBootstrap = false, scriptletsOnly } = config;
	try {
		setup.pending && await setup.pending;
	} catch (e) {
		console.error("[adblocker] not ready for cosmetic injection", e);
		return;
	}
	const { tabId, frameId, parentFrameId, documentId, url } = details;
	const parsed = parseWithCache(url);
	const domain = parsed.domain || "";
	const hostname = parsed.hostname || "";
	const options = store_default.get(Options);
	if (getPausedDetails(options, hostname)) return false;
	const tabHostname = tabStats.get(tabId)?.hostname;
	if (tabHostname && getPausedDetails(options, tabHostname)) return false;
	const engine = get(MAIN_ENGINE);
	let ancestors = void 0;
	if (SUBFRAME_SCRIPTING.enabled && typeof parentFrameId === "number") ancestors = framesHierarchy.ancestors({
		tabId,
		frameId,
		parentFrameId,
		documentId
	}, {
		domain,
		hostname
	});
	{
		const { matches } = engine.matchCosmeticFilters({
			domain,
			hostname,
			url,
			ancestors,
			classes: config.classes,
			hrefs: config.hrefs,
			ids: config.ids,
			getInjectionRules: isBootstrap,
			getExtendedRules: isBootstrap,
			getRulesFromHostname: isBootstrap,
			getPureHasRules: true,
			getRulesFromDOM: !isBootstrap,
			callerContext: { tabId }
		});
		const styleFilters = [];
		const scriptFilters = [];
		for (const { filter, exception } of matches) if (exception === void 0) if (filter.isScriptInject()) scriptFilters.push(filter);
		else styleFilters.push(filter);
		if (isBootstrap) injectScriptlets(scriptFilters, hostname, details);
		if (scriptletsOnly) return;
		const { styles, extended } = engine.injectCosmeticFilters(styleFilters, {
			url,
			injectScriptlets: isBootstrap,
			injectExtended: isBootstrap,
			injectPureHasSafely: true,
			allowGenericHides: false,
			getBaseRules: false
		});
		if (styles) injectStyles(styles, details);
		if (extended && extended.length > 0) chrome.tabs.sendMessage(tabId, {
			action: "evaluateExtendedSelectors",
			extended
		}, { frameId }).catch(() => {});
	}
	if (isBootstrap) {
		const { styles } = engine.getCosmeticsFilters({
			domain,
			hostname,
			url,
			getBaseRules: true,
			getInjectionRules: false,
			getExtendedRules: false,
			getRulesFromDOM: false,
			getRulesFromHostname: false
		});
		injectStyles(styles, details);
	}
}
chrome.webNavigation.onCommitted.addListener((details) => injectCosmetics(details, { bootstrap: true }), { url: [{ urlPrefix: "http://" }, { urlPrefix: "https://" }] });
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
	if (msg.action === "injectCosmetics" && sender.tab) {
		injectCosmetics({
			url: !sender.url.startsWith("http") ? sender.origin : sender.url,
			tabId: sender.tab.id,
			frameId: sender.frameId,
			documentId: sender.documentId
		}, msg).then(sendResponse);
		return true;
	}
});
chrome.webRequest?.onResponseStarted.addListener((details) => {
	if (details.tabId === -1) return;
	if (details.type !== "main_frame" && details.type !== "sub_frame") return;
	if (!details.documentId) return;
	injectCosmetics(details, { bootstrap: true });
}, { urls: ["http://*/*", "https://*/*"] });
//#endregion
