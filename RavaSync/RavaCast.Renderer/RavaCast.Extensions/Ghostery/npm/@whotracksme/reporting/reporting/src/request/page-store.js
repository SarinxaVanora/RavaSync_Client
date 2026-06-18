import logger_default from "../logger.js";
import ChromeStorageMap from "./utils/chrome-storage-map.js";
//#region node_modules/@whotracksme/reporting/reporting/src/request/page-store.js
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
var PAGE_TTL = 1e3 * 60 * 60;
var BFCACHE_TTL_MS = 600 * 1e3;
var FLUSH_THROTTLE_MS = 30 * 1e3;
var PAGE_LOADING_STATE = {
	COMMITTED: "committed",
	COMPLETE: "complete"
};
function makePageActive(page, active) {
	if (active && page.activeFrom === 0) page.activeFrom = Date.now();
	else if (!active && page.activeFrom > 0) {
		page.activeTime += Date.now() - page.activeFrom;
		page.activeFrom = 0;
	}
}
function createPage({ tabId, documentId, url, isPrivate, active }) {
	return {
		id: tabId,
		documentId,
		documentIds: [documentId],
		url,
		isPrivate: !!isPrivate,
		isPrivateServer: false,
		created: Date.now(),
		destroyed: null,
		stageAfter: null,
		state: PAGE_LOADING_STATE.COMMITTED,
		activeTime: 0,
		activeFrom: active ? Date.now() : 0,
		requestStats: {},
		annotations: {},
		counter: 0
	};
}
var PageStore = class {
	#notifyPageStageListeners;
	#pages;
	#documentIndex;
	#tabContext;
	#lastFlush = 0;
	constructor({ notifyPageStageListeners }) {
		this.#pages = new ChromeStorageMap({
			storageKey: "wtm-request-reporting:page-store:pages",
			ttlInMs: PAGE_TTL
		});
		this.#documentIndex = /* @__PURE__ */ new Map();
		this.#tabContext = /* @__PURE__ */ new Map();
		this.#notifyPageStageListeners = notifyPageStageListeners;
	}
	async init() {
		await this.#pages.isReady;
		for (const page of this.#pages.values()) for (const docId of page.documentIds) this.#documentIndex.set(docId, page);
		for (const tab of await chrome.tabs.query({})) this.#onTabCreated(tab);
		chrome.tabs.onCreated.addListener(this.#onTabCreated);
		chrome.tabs.onUpdated.addListener(this.#onTabUpdated);
		chrome.tabs.onRemoved.addListener(this.#onTabRemoved);
		chrome.tabs.onActivated.addListener(this.#onTabActivated);
		chrome.webNavigation.onCommitted.addListener(this.#onNavigationCommitted);
		chrome.webNavigation.onCompleted.addListener(this.#onNavigationCompleted);
		chrome.windows?.onFocusChanged?.addListener(this.#onWindowFocusChanged);
	}
	unload() {
		this.#documentIndex.clear();
		this.#tabContext.clear();
		chrome.tabs.onCreated.removeListener(this.#onTabCreated);
		chrome.tabs.onUpdated.removeListener(this.#onTabUpdated);
		chrome.tabs.onRemoved.removeListener(this.#onTabRemoved);
		chrome.tabs.onActivated.removeListener(this.#onTabActivated);
		chrome.webNavigation.onCommitted.removeListener(this.#onNavigationCommitted);
		chrome.webNavigation.onCompleted.removeListener(this.#onNavigationCompleted);
		chrome.windows?.onFocusChanged?.removeListener(this.#onWindowFocusChanged);
	}
	checkIfEmpty() {
		return this.#pages.countNonExpiredKeys() === 0;
	}
	findPageForTab(tabId) {
		for (const page of this.#pages.values()) if (page.id === tabId) return page;
	}
	async flush() {
		const live = await this.#collectLiveDocumentIds();
		const now = Date.now();
		this.#lastFlush = now;
		for (const page of this.#pages.values()) if (page.documentIds.some((d) => live.has(d))) {
			if (page.stageAfter !== null) {
				page.stageAfter = null;
				this.#pages.set(page.documentId, page);
			}
		} else if (page.stageAfter === null) {
			page.stageAfter = now + BFCACHE_TTL_MS;
			this.#pages.set(page.documentId, page);
		} else if (now >= page.stageAfter) this.#stagePage(page);
	}
	async #collectLiveDocumentIds() {
		const live = /* @__PURE__ */ new Set();
		for (const tab of await chrome.tabs.query({})) try {
			const frames = await chrome.webNavigation.getAllFrames({ tabId: tab.id });
			for (const frame of frames) if (frame.documentId) live.add(frame.documentId);
		} catch (e) {
			logger_default.debug("PageStore: getAllFrames failed for tab", tab.id, e);
		}
		return live;
	}
	#stagePage(page) {
		for (const docId of page.documentIds) this.#documentIndex.delete(docId);
		this.#pages.delete(page.documentId);
		const snapshot = structuredClone(page);
		makePageActive(snapshot, false);
		snapshot.destroyed = Date.now();
		this.#notifyPageStageListeners(snapshot);
	}
	#indexDocument(page, documentId) {
		if (!page.documentIds.includes(documentId)) page.documentIds.push(documentId);
		this.#documentIndex.set(documentId, page);
	}
	#setTabActive(tabId, active) {
		const ctx = this.#tabContext.get(tabId);
		if (ctx) ctx.active = active;
		for (const page of this.#pages.values()) if (page.id === tabId) {
			makePageActive(page, active);
			this.#pages.set(page.documentId, page);
		}
	}
	#onTabCreated = (tab) => {
		this.#tabContext.set(tab.id, {
			isPrivate: !!tab.incognito,
			active: !!tab.active
		});
	};
	#onTabUpdated = (tabId, info, tab) => {
		if (info.discarded) {
			this.#markTabGone(tabId);
			return;
		}
		const ctx = {
			isPrivate: !!tab.incognito,
			active: !!tab.active
		};
		this.#tabContext.set(tabId, ctx);
		for (const page of this.#pages.values()) if (page.id === tabId) {
			page.isPrivate = ctx.isPrivate;
			makePageActive(page, ctx.active);
			this.#pages.set(page.documentId, page);
		}
	};
	#onTabRemoved = (tabId) => {
		this.#tabContext.delete(tabId);
		this.#markTabGone(tabId);
	};
	#markTabGone(tabId) {
		for (const page of this.#pages.values()) if (page.id === tabId) {
			page.stageAfter = 0;
			this.#pages.set(page.documentId, page);
		}
		this.flush().catch((e) => logger_default.debug("PageStore: flush after tab gone failed", e));
	}
	#onTabActivated = ({ previousTabId, tabId }) => {
		if (previousTabId) this.#setTabActive(previousTabId, false);
		else for (const otherTabId of this.#tabContext.keys()) if (otherTabId !== tabId) this.#setTabActive(otherTabId, false);
		this.#setTabActive(tabId, true);
	};
	#onWindowFocusChanged = async (focusedWindowId) => {
		for (const { id, windowId } of await chrome.tabs.query({ active: true })) this.#setTabActive(id, windowId === focusedWindowId);
	};
	#onNavigationCommitted = ({ frameId, tabId, documentId, parentDocumentId, url }) => {
		if (frameId !== 0) {
			if (this.#documentIndex.has(documentId)) return;
			const owner = parentDocumentId && this.#documentIndex.get(parentDocumentId);
			if (!owner) return;
			this.#indexDocument(owner, documentId);
			this.#pages.set(owner.documentId, owner);
			return;
		}
		let page = this.#pages.get(documentId);
		if (!page) page = createPage({
			tabId,
			documentId,
			url,
			...this.#tabContext.get(tabId) || {}
		});
		page.url = url;
		page.stageAfter = null;
		this.#indexDocument(page, documentId);
		this.#pages.set(documentId, page);
	};
	#onNavigationCompleted = ({ frameId, documentId }) => {
		if (frameId !== 0) return;
		const page = this.#pages.get(documentId);
		if (page) {
			page.state = PAGE_LOADING_STATE.COMPLETE;
			this.#pages.set(documentId, page);
		}
		if (Date.now() - this.#lastFlush < FLUSH_THROTTLE_MS) return;
		this.flush().catch((e) => logger_default.debug("PageStore: flush failed", e));
	};
	getPageForRequest({ documentId, parentDocumentId, documentLifecycle }) {
		if (documentLifecycle === "prerender") return null;
		if (documentId) {
			const direct = this.#documentIndex.get(documentId);
			if (direct) return direct;
		}
		if (parentDocumentId) {
			const owner = this.#documentIndex.get(parentDocumentId);
			if (owner) {
				if (documentId) {
					this.#indexDocument(owner, documentId);
					this.#pages.set(owner.documentId, owner);
				}
				return owner;
			}
		}
		return null;
	}
};
//#endregion
export { PageStore as default };
