import logger_default from "./logger.js";
import { equalityCanBeProven, flattenObject, isNil, requireInt, requireParam, split0 } from "./utils.js";
import SelfCheck from "./self-check.js";
import { DnsResolver } from "./network.js";
import ActivityEstimator from "./activity-estimator.js";
import Observable from "./observable.js";
import { analyzePageStructure } from "./page-structure.js";
import EventListenerQueue from "./event-listener-queue.js";
//#region node_modules/@whotracksme/reporting/reporting/src/pages.js
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
/**
* Marker for lazy variables where the initialization was aborted.
*
* (exported for tests only)
*/
var CANCEL_LAZY_VAR = { _tag: "CANCEL_LAZY_VAR" };
/**
* Recursively traverses the given object and performs two operations:
* - pending or cancelled lazy vars will removed
* - resolved lazy vars will be inlined
*
* For more details on lazy variables, see Pages#_setLazyVar.
*
* Notes:
* - the implementation assumes that objects are free of cycles
* - as part of the serialization, explicit mappings to "undefined" will be omitted
*   (this is consistent with JSON.stringify; if you need keys with optional values,
*    you can map to "null" instead)
*
* (exported for tests only)
*/
function stripLazyVars(obj) {
	if (isNil(obj)) return obj;
	if (Array.isArray(obj)) return obj.map(stripLazyVars);
	if (obj === CANCEL_LAZY_VAR) return;
	if (Object(obj) !== obj || obj.constructor === Date) return obj;
	const result = {};
	const set = (key, val) => {
		const v = stripLazyVars(val);
		if (v !== void 0) result[key] = v;
	};
	for (const [key, val] of Object.entries(obj)) if (val?._pending) {
		if (val.result && val.result !== CANCEL_LAZY_VAR) set(key, val.result);
	} else set(key, val);
	return result;
}
function isRealPage(url) {
	return url && (url.startsWith("http://") || url.startsWith("https://"));
}
function sameHostname(url1, url2) {
	try {
		return new URL(url1).hostname === new URL(url2).hostname;
	} catch (e) {
		return false;
	}
}
/**
* The API should match an ES6 map.
*/
var OpenTabs = class {
	constructor(pages) {
		this._map = /* @__PURE__ */ new Map();
		this._pages = pages;
	}
	has(tabId) {
		return this._map.has(tabId);
	}
	get(tabId) {
		return this._map.get(tabId);
	}
	keys() {
		return this._map.keys();
	}
	values() {
		return this._map.values();
	}
	entries() {
		return this._map.entries();
	}
	set(tabId, value) {
		const result = this._map.set(tabId, value);
		this._pages._onTabChanged(tabId);
		return result;
	}
	delete(tabId) {
		const wasDeleted = this._map.delete(tabId);
		if (wasDeleted) this._pages._onTabChanged(tabId);
		return wasDeleted;
	}
	*[Symbol.iterator]() {
		for (const entry of this._map) yield entry;
	}
	/**
	* This will not trigger tab change events.
	*/
	restoreTabsFromMap(openTabs) {
		this._map = openTabs;
	}
};
/**
* An abstraction of the active tab. Note that we assume that there is a single
* active tab, which already different from what the browser sees: the browser
* sees multiple windows, and generally one active tab per window.
*
* The definition of the active tab used in this class is closer to what you
* would get by executing:
* ```
* chrome.tabs.query({ active: true, currentWindow: true })
* ```
*/
var ActiveTab = class ActiveTab {
	constructor(pages) {
		this._pages = pages;
		this._state = {
			tabId: -1,
			windowId: -1,
			activeTabInWindow: /* @__PURE__ */ new Map(),
			lastUpdated: 0
		};
		this._pendingFlush = null;
	}
	get tabId() {
		return this._state.tabId;
	}
	get activeWindowId() {
		return this._state.windowId;
	}
	updateActiveTab({ tabId, windowId, now = Date.now() }) {
		const previousTabId = this._state.tabId;
		this._state.tabId = tabId;
		this._state.windowId = windowId;
		this._state.lastUpdated = now;
		this.saveActiveTabInWindow({
			tabId,
			windowId,
			now
		});
		this._markDirty();
		if (tabId !== previousTabId) this._pages._activeTab_onActiveTabChanged({
			tabId,
			previousTabId,
			now
		});
	}
	saveActiveTabInWindow({ tabId, windowId, now = Date.now() }) {
		if (windowId !== -1) {
			this._state.activeTabInWindow.set(windowId, tabId);
			this._state.lastUpdated = now;
			this._markDirty();
		}
	}
	focusWindow(windowId, now = Date.now()) {
		const tabId = this._state.activeTabInWindow.get(windowId) ?? -1;
		this.updateActiveTab({
			tabId,
			windowId,
			now
		});
	}
	removeWindow(windowId, now = Date.now()) {
		if (this._state.windowId === windowId) {
			this._state.tabId = -1;
			this._state.windowId = -1;
		}
		this._state.activeTabInWindow.delete(windowId);
		this._state.lastUpdated = now;
		this._markDirty();
	}
	_markDirty() {
		if (this._pendingFlush === null) this._pendingFlush = setTimeout(() => {
			this.flush();
		}, 0);
	}
	flush() {
		if (this._pendingFlush !== null) {
			this._pendingFlush = null;
			this._pages._activeTab_onInternalStateChanged();
		}
	}
	serialize() {
		return {
			tabId: this._state.tabId,
			windowId: this._state.windowId,
			activeTabInWindow__serialized__: [...this._state.activeTabInWindow],
			lastUpdated: this._state.lastUpdated
		};
	}
	restore(state) {
		this._state = ActiveTab.ensureValidState({
			tabId: state.tabId,
			windowId: state.windowId,
			activeTabInWindow: new Map(state.activeTabInWindow__serialized__),
			lastUpdated: state.lastUpdated || 0
		});
	}
	static ensureValidState(state) {
		const { tabId, windowId, activeTabInWindow, lastUpdated } = state || {};
		requireInt(tabId, "tabId");
		requireInt(windowId, "windowId");
		requireInt(lastUpdated, "lastUpdated");
		for (const [key, value] of [...activeTabInWindow]) {
			requireInt(key, "activeTabInWindow[key]");
			requireInt(value, "activeTabInWindow[value]");
		}
		return state;
	}
	selfChecks(check = new SelfCheck()) {
		try {
			ActiveTab.ensureValidState(this._state);
		} catch (e) {
			check.fail("Invalid state", {
				state: this._state,
				reason: e.message
			});
			return check;
		}
		const { tabId, windowId, activeTabInWindow } = this._state;
		if (tabId !== -1 && activeTabInWindow.get(windowId) !== tabId) check.warn("tabId and activeTabInWindow out of sync");
		return check;
	}
};
/**
* Responsible for aggregating information about open pages.
*
* 1) Aggregation
*
* It works by aggregating information from available extension APIs.
* The implementation needs to be fault-tolerant to recover from inconsistent
* states, loss of the state (if the service worker gets killed), lack of APIs
* (e.g. on Safari), and fail gracefully in situations where the listener is
*
* For error recovery, it may sacrifice precision by deleting information
* that is most likely outdated or even wrong. The overall goal of this class
* is to provide information that is reliable and matches or exceeds what the
* chrome.tabs.query IP would get.
*
* 2) Observerable "page" events
*
* In addition to providing access to the current state, the class will emit
* events that are domain-specific and higher-level than the browser APIs.
* "addObserver" can used by other classed to register a (synchronous)
* event handler.
*
* Note: These events are not intended to provide complete coverage (e.g.
* actions in incognito tabs will be omitted). If you need complete information,
* it is recommended to consume the browser APIs directly.
*
* ----------------------------------------------------------------------
*
* Cross-browser support:
*
* List of Safari quirks:
* https://developer.apple.com/documentation/safariservices/safari_web_extensions/assessing_your_safari_web_extension_s_browser_compatibility
*
* TODO: how to deal with speculative requests? Is it a problem?
* https://developer.mozilla.org/en-US/docs/Mozilla/Add-ons/WebExtensions/API/webRequest#speculative_requests
*/
var Pages = class Pages {
	constructor({ config, urlAnalyzer, newPageApprover, pageSessionStore, dnsResolver = new DnsResolver() }) {
		this.isActive = false;
		this.urlAnalyzer = requireParam(urlAnalyzer);
		this.newPageApprover = requireParam(newPageApprover);
		this._observable = new Observable();
		this.openTabs = new OpenTabs(this);
		this.activeTab = new ActiveTab(this);
		this.activityEstimator = new ActivityEstimator({
			onActivityUpdated: this._activityEstimator_onActivityUpdated.bind(this),
			onInternalStateChanged: this._activityEstimator_onInternalStateChanged.bind(this)
		});
		this.dnsResolver = requireParam(dnsResolver);
		this.sessionStore = requireParam(pageSessionStore);
		this._pageIdGenerator = 1 + Math.floor(Math.random() * 4294967296);
		this.chrome = config.pages?.chrome || globalThis.chrome || {};
		if (!this.chrome?.tabs?.query) logger_default.warn("chrome.tabs.query API missing");
		this.maxRedirects = 8;
		this.langStats = {};
		this.verboseLogging = config.pages?.verbose ?? false;
		this.isPageLoadMethodReliable = this.chrome.webNavigation?.onHistoryStateUpdated?.addListener && this.chrome.webNavigation?.onHistoryStateUpdated?.removeListener;
		this.enableFirefoxAndroidQuirks = true;
	}
	addObserver(onPageEventCallback) {
		this._observable.addObserver(onPageEventCallback);
	}
	removeObserver(onPageEventCallback) {
		this._observable.removeObserver(onPageEventCallback);
	}
	notifyObservers(event) {
		this._observable.notifyObservers(event);
	}
	async init() {
		this.isActive = true;
		if (!this._ready) this._ready = new Promise((done) => {
			(async () => {
				try {
					const pendingInit = this.sessionStore.init();
					const pendingTabSyncParams = this._prepareSyncWithOpenTabsFromTabsAPI();
					try {
						await pendingInit;
						this._restoreSession(this.sessionStore.getEntries());
					} catch (e) {
						logger_default.warn("Failed to restore previous state from in-memory session (if there was any)", e);
						this.sessionStore.clear();
					}
					try {
						this._syncWithOpenTabsFromTabsAPI(await pendingTabSyncParams);
					} catch (e) {
						logger_default.warn("Failed to sync with open tabs", e);
					}
					if (this.isActive) {
						eventListenerQueue.replayEvents();
						this._setupListener();
					}
					this._updateSession();
					logger_default.debug("Successfully initialized. Forcing full page aggregator sync...");
					this.notifyObservers({ type: "full-sync" });
				} finally {
					eventListenerQueue.close();
					done();
				}
			})();
		});
	}
	async blockUntilFullInit() {
		if (!this.isActive) throw new Error("Not active (try to call \"init\" first)");
		await this._ready;
	}
	async syncWithBrowser() {
		await this.blockUntilFullInit();
		this._syncWithOpenTabsFromTabsAPI(await this._prepareSyncWithOpenTabsFromTabsAPI());
	}
	_restoreSession(session) {
		if (!session || Object.keys(session).length === 0) {
			logger_default.debug("Found no previous state about pages to restore");
			return;
		}
		const { dnsResolver, activeTab, activityEstimator, ...openTabs } = session;
		this.activeTab.restore(activeTab);
		this.dnsResolver.restore(dnsResolver);
		this.activityEstimator.restore(activityEstimator);
		this._restoreTabs(openTabs);
	}
	_serializeFullSession() {
		return {
			dnsResolver: this.dnsResolver.serialize(),
			activeTab: this.activeTab.serialize(),
			activityEstimator: this.activityEstimator.serialize(),
			...this._serializeTabs()
		};
	}
	_updateSession() {
		if (this.sessionStore.isReady()) this.sessionStore.replaceItems(this._serializeFullSession());
	}
	_onTabChanged(tabId) {
		if (this.verboseLogging) console.trace("tabChanged:", tabId, ":", this.openTabs.get(tabId));
		const content = this.openTabs.get(tabId);
		if (tabId === this.activeTab.tabId) this.activityEstimator.updateActiveUrl(content?.url || null);
		if (this.sessionStore.isReady()) if (content) this.sessionStore.set(tabId, this._serializeTabContent(content));
		else this.sessionStore.remove(tabId);
	}
	_activeTab_onActiveTabChanged({ tabId, previousTabId, now = Date.now() }) {
		if (this.verboseLogging) {
			const url = this.openTabs.get(tabId)?.url;
			console.trace("activeTabChanged:", {
				url,
				from: {
					tabId: previousTabId,
					url: this.openTabs.get(previousTabId)?.url
				},
				to: {
					tabId,
					url
				}
			});
		}
		const url = this.openTabs.get(tabId)?.url;
		this.activityEstimator.updateActiveUrl(url || null, now);
	}
	_activeTab_onInternalStateChanged() {
		if (this.sessionStore.isReady()) this.sessionStore.set("activeTab", this.activeTab.serialize());
	}
	_activityEstimator_onActivityUpdated(urls) {
		if (this.isActive && urls.length > 0) this.notifyObservers({
			type: "activity-updated",
			urls,
			activityEstimator: this.activityEstimator
		});
	}
	_activityEstimator_onInternalStateChanged() {
		if (this.sessionStore.isReady()) this.sessionStore.set("activityEstimator", this.activityEstimator.serialize());
	}
	_serializeTabContent(tabContent) {
		return stripLazyVars(tabContent);
	}
	_deserializeTabContent(tabContent) {
		const result = {};
		Object.entries(tabContent).forEach(([key, val]) => {
			if (val === void 0) logger_default.warn("\"undefined\" mappings should not have been serialized:", key, "(drop and continue...)");
			else if (val?._pending) logger_default.warn("Lazy variable should not have been serialized (since they are promises):", key, "->", val, "(drop and continue...)");
			else result[key] = val;
		});
		return result;
	}
	_serializeTabs() {
		const result = {};
		for (const [tabId, tabContent] of this.openTabs) result[tabId] = this._serializeTabContent(tabContent);
		return result;
	}
	_restoreTabs(openTabs) {
		if (openTabs) {
			const deserializedTabs = Object.entries(openTabs).map(([tabId, tabContent]) => [tabId, this._deserializeTabContent(tabContent)]);
			this.openTabs.restoreTabsFromMap(new Map(deserializedTabs));
		}
	}
	async _tryChromeTabsQuery(...args) {
		if (this.chrome?.tabs?.query) return this.chrome.tabs.query(...args);
		if (!this._warnedAboutChromeTabsQueryApi) {
			this._warnedAboutChromeTabsQueryApi = true;
			logger_default.warn("chrome.tabs.query not available on this platform");
		}
	}
	async _tryChromeWindowsGetLastFocused(...args) {
		if (this.chrome?.windows?.getLastFocused) return this.chrome.windows.getLastFocused(...args);
		if (!this._warnedAboutChromeWindowsGetLastFocusedApi) {
			this._warnedAboutChromeWindowsGetLastFocusedApi = true;
			logger_default.warn("chrome.windows.getLastFocused not available on this platform");
		}
	}
	async _prepareSyncWithOpenTabsFromTabsAPI() {
		const [tabs, lastFocusedWindow] = await Promise.all([this._tryChromeTabsQuery({}), this._tryChromeWindowsGetLastFocused()]);
		return {
			tabs: tabs || [],
			lastActiveWindowId: lastFocusedWindow?.id || null
		};
	}
	_syncWithOpenTabsFromTabsAPI({ tabs, lastActiveWindowId, now = Date.now() }) {
		const activeTabCandidates = [];
		const unmatchedTabIds = new Set(this.openTabs.keys());
		for (const tab of tabs) {
			if (tab.incognito || !isRealPage(tab.url)) continue;
			let oldEntry = null;
			if (unmatchedTabIds.delete(tab.id)) {
				oldEntry = this.openTabs.get(tab.id);
				if (oldEntry.url !== tab.url) {
					logger_default.warn("Ignoring tab after a URL mismatch:", {
						oldEntry,
						newEntry: tab
					});
					oldEntry = null;
				}
			}
			const { id: tabId, windowId, status, title, url } = tab;
			const entry = {
				...oldEntry,
				status,
				title,
				url,
				windowId,
				lastUpdatedAt: now,
				pageId: ++this._pageIdGenerator
			};
			this._initVisibility(tabId, entry, now);
			if (tab.active && isRealPage(tab.url)) {
				this.activeTab.saveActiveTabInWindow({
					tabId,
					windowId,
					now
				});
				if (windowId === lastActiveWindowId || isNil(lastActiveWindowId)) {
					activeTabCandidates.push({
						tabId,
						windowId
					});
					if (tab.status === "complete") this._initAllOptionalFields(tabId, entry, now);
				}
			}
			this.openTabs.set(tabId, entry);
		}
		if (activeTabCandidates.length > 1 && this.chrome?.windows?.getLastFocused) logger_default.warn("Multiple tabs are being detected as active", "(since the chrome.windows API is available, this is unexpected)", activeTabCandidates);
		if (activeTabCandidates.length === 1) {
			const { tabId, windowId } = activeTabCandidates[0];
			this.activeTab.updateActiveTab({
				tabId,
				windowId,
				now
			});
		} else this.activeTab.updateActiveTab({
			tabId: -1,
			windowId: -1,
			now
		});
		if (unmatchedTabIds.size > 0) unmatchedTabIds.forEach((tabId) => this.openTabs.delete(tabId));
	}
	unload() {
		this.isActive = false;
		if (this._removeListeners) {
			this._removeListeners();
			this._removeListeners = null;
		}
		this.activeTab.flush();
		this.activityEstimator.flush();
		eventListenerQueue.close();
	}
	describe(now = Date.now()) {
		const openTabs = {};
		for (const [tabId, pageInfo] of this.openTabs) if (isRealPage(pageInfo.url)) {
			const isActive = tabId === this.activeTab.tabId;
			openTabs[tabId] = this._describePage(pageInfo, isActive, now);
		}
		let activeTab;
		if (this.activeTab.tabId !== -1 && openTabs[this.activeTab.tabId]) activeTab = {
			tabId: this.activeTab.tabId,
			tab: openTabs[this.activeTab.tabId]
		};
		return {
			activeTab,
			openTabs
		};
	}
	describeTab(tabId, now = Date.now()) {
		const pageInfo = this.openTabs.get(tabId);
		if (!pageInfo || !isRealPage(pageInfo.url)) return null;
		const isActive = tabId === this.activeTab.tabId;
		return this._describePage(pageInfo, isActive, now);
	}
	/**
	* Either the (non-negative) id of the tab that is assumed to be active,
	* or TAB_ID_NONE, which is guaranteed to be -1.
	*/
	getActiveTabId() {
		return this.activeTab.tabId;
	}
	_describePage(pageInfo, isActive, now = Date.now()) {
		const result = {
			status: pageInfo.status,
			title: pageInfo.title,
			url: pageInfo.url,
			visibility: pageInfo.visibility,
			windowId: pageInfo.windowId,
			lastUpdatedAt: pageInfo.lastUpdatedAt,
			isActive
		};
		if (pageInfo.previousUrl) result.ref = pageInfo.previousUrl;
		if (this.isPageLoadMethodReliable && pageInfo.pageLoadMethod) result.pageLoadMethod = pageInfo.pageLoadMethod;
		this._tryUnwrapLazyVar(pageInfo.language, (value) => {
			result.lang = value;
		});
		this._tryUnwrapLazyVar(pageInfo.pageStructure, (value) => {
			result.preDoublefetch = value;
			if (value.noindex) result.visibility = "private";
		});
		if (pageInfo.redirects) result.redirects = pageInfo.redirects;
		if (pageInfo.search) result.search = {
			category: pageInfo.search.category,
			query: pageInfo.search.query,
			depth: pageInfo.search.depth
		};
		if (isActive && pageInfo.url) result.activity = this.activityEstimator.estimate(pageInfo.url, now);
		return result;
	}
	_setupListener() {
		if (this._removeListeners) return;
		const cleanup = [];
		const wrapHandler = (handler) => {
			handler = handler.bind(this);
			return (...args) => {
				if (this.isActive) handler(...args);
			};
		};
		if (this.chrome.tabs) for (const type of [
			"onCreated",
			"onUpdated",
			"onRemoved",
			"onActivated"
		]) {
			const handler = wrapHandler(this[`chrome_tabs_${type}`]);
			if (this.chrome.tabs[type]?.addListener && this.chrome.tabs[type]?.removeListener) {
				this.chrome.tabs[type].addListener(handler);
				cleanup.push(() => this.chrome.tabs[type].removeListener(handler));
			}
		}
		if (this.chrome.webRequest) for (const type of [
			"onBeforeRequest",
			"onBeforeRedirect",
			"onResponseStarted",
			"onCompleted",
			"onAuthRequired"
		]) {
			const handler = wrapHandler(this[`chrome_webRequest_${type}`]);
			if (this.chrome.webRequest[type]?.addListener && this.chrome.webRequest[type]?.removeListener) {
				try {
					this.chrome.webRequest[type].addListener(handler, { urls: ["<all_urls>"] }, ["responseHeaders"]);
				} catch (e) {
					this.chrome.webRequest[type].addListener(handler, { urls: ["<all_urls>"] });
				}
				cleanup.push(() => this.chrome.webRequest[type].removeListener(handler));
			} else logger_default.info(`chrome.webRequest.${type} API is unavailable`);
		}
		if (this.chrome.webNavigation) for (const type of [
			"onCreatedNavigationTarget",
			"onBeforeNavigate",
			"onCommitted",
			"onHistoryStateUpdated"
		]) {
			const handler = wrapHandler(this[`chrome_webNavigation_${type}`]);
			if (this.chrome.webNavigation[type]?.addListener && this.chrome.webNavigation[type]?.removeListener) {
				this.chrome.webNavigation[type].addListener(handler);
				cleanup.push(() => this.chrome.webNavigation[type].removeListener(handler));
			} else logger_default.info(`chrome.webNavigation.${type} API is unavailable`);
		}
		if (this.chrome.windows) for (const type of ["onRemoved", "onFocusChanged"]) {
			const handler = wrapHandler(this[`chrome_windows_${type}`]);
			if (this.chrome.windows[type]?.addListener && this.chrome.windows[type]?.removeListener) {
				this.chrome.windows[type].addListener(handler);
				cleanup.push(() => this.chrome.windows[type].removeListener(handler));
			} else logger_default.info(`chrome.windows.${type} API is unavailable`);
		}
		this._removeListeners = () => {
			cleanup.forEach((f) => f());
		};
	}
	chrome_tabs_onCreated(tab) {
		if (tab.incognito) {
			this.openTabs.delete(tab.id);
			return;
		}
		if (tab.openerTabId !== void 0) {
			if (tab.pendingUrl && !isRealPage(tab.pendingUrl)) return;
			const openedFrom = this.openTabs.get(tab.openerTabId);
			if (openedFrom) {
				const entry = {
					status: "created",
					...this.openTabs.get(tab.id),
					windowId: tab.windowId,
					lastUpdatedAt: Date.now(),
					pageId: ++this._pageIdGenerator,
					openedFrom: { ...openedFrom }
				};
				this.openTabs.set(tab.id, entry);
			}
			return;
		}
		if (this.enableFirefoxAndroidQuirks && tab.active === false && this.activeTab.tabId !== -1 && tab.url === "about:blank" && !this.openTabs.has(tab.id)) {
			const activeTab = this.openTabs.get(this.activeTab.tabId);
			if (!activeTab || !isRealPage(activeTab.url)) return;
			const now = Date.now();
			const entry = {
				status: "created",
				windowId: tab.windowId,
				lastUpdatedAt: now,
				pageId: ++this._pageIdGenerator,
				_unverifiedOpener__FirefoxAndroid__: {
					openedFrom: { ...activeTab },
					lastUpdatedAt: now,
					expectedOriginUrl: activeTab.url
				}
			};
			this.openTabs.set(tab.id, entry);
		}
	}
	chrome_tabs_onUpdated(tabId, changeInfo, tab) {
		if (tab.incognito) return;
		const now = Date.now();
		const { windowId } = tab;
		if (tab.active) if (this.activeTab.activeWindowId === windowId) this.activeTab.updateActiveTab({
			tabId,
			windowId,
			now
		});
		else this.activeTab.saveActiveTabInWindow({
			tabId,
			windowId,
			now
		});
		let oldTabEntry = this.openTabs.get(tabId);
		if (changeInfo.status === "complete") {
			const entry = {
				...oldTabEntry,
				status: "complete",
				title: tab.title,
				url: tab.url,
				windowId,
				lastUpdatedAt: now
			};
			if (!entry.pageId || entry.url !== oldTabEntry.url) entry.pageId = ++this._pageIdGenerator;
			this._initAllOptionalFields(tabId, entry, now);
			this.openTabs.set(tabId, entry);
			if (entry.search?.depth === 1) {
				const { url, search, redirects = [] } = entry;
				this.notifyObservers({
					type: "safe-search-landing",
					tabId,
					details: {
						from: {
							category: search.category,
							query: search.query
						},
						to: { targetUrl: url },
						redirects
					}
				});
			}
			return;
		}
		if (tab.status === "loading") {
			let entry;
			if (oldTabEntry && (oldTabEntry.url === tab.url || oldTabEntry.url === "about:blank" && oldTabEntry.status === "loading")) {
				entry = {
					...oldTabEntry,
					title: tab.title,
					url: tab.url,
					windowId,
					lastUpdatedAt: now
				};
				const red = entry._unverifiedRedirects;
				if (red && tab.url !== "about:blank") {
					if (red[red.length - 1].to === tab.url) {
						entry.redirects = [...red];
						logger_default.debug("confirmed redirect:", red, "->", tab.url);
					} else logger_default.warn("rejected redirects:", { ...entry });
					delete entry._unverifiedRedirects;
				}
			} else {
				entry = {
					status: "loading",
					pageLoadMethod: "full-page-load",
					title: tab.title,
					url: tab.url,
					windowId,
					lastUpdatedAt: now,
					pageId: ++this._pageIdGenerator
				};
				if (oldTabEntry) {
					if (oldTabEntry.status !== "created") entry.openedFrom = { ...oldTabEntry };
					else if (oldTabEntry.openedFrom) entry.openedFrom = { ...oldTabEntry.openedFrom };
					const redirects = entry.openedFrom?.pendingRedirects || [];
					if (redirects.length > 0) {
						if (redirects[redirects.length - 1].to === tab.url) entry.redirects = [...redirects];
						else if (tab.url === "about:blank") {
							logger_default.debug("delaying redirect matching:", redirects);
							entry._unverifiedRedirects = [...redirects];
						}
					}
					if (entry.openedFrom?.status === "redirecting" && entry.openedFrom.openedFrom) entry.openedFrom = { ...entry.openedFrom.openedFrom };
					const previousUrl = entry.openedFrom?.url;
					if (isRealPage(previousUrl)) entry.previousUrl = previousUrl;
				}
			}
			if (!entry.search && entry.openedFrom?.search && entry.previousUrl) {
				if (entry.openedFrom.search.depth === 0 || entry.openedFrom.search.depth === 1 && sameHostname(entry.url, entry.previousUrl)) entry.search = {
					...entry.openedFrom.search,
					depth: entry.openedFrom.search.depth + 1,
					lastUpdatedAt: now
				};
			}
			this.openTabs.set(tabId, entry);
			return;
		}
		if (oldTabEntry) {
			const entry = {
				...oldTabEntry,
				lastUpdatedAt: now
			};
			let updated = false;
			if (tab.title && tab.title !== entry.title) {
				entry.title = tab.title;
				entry.pageId = ++this._pageIdGenerator;
				updated = true;
			}
			if (tab.url && tab.url !== entry.url) {
				entry.url = tab.url;
				entry.pageId = ++this._pageIdGenerator;
				updated = true;
			}
			this.openTabs.set(tabId, entry);
			if (updated) this.notifyObservers({
				type: "page-updated",
				tabId
			});
			return;
		}
		logger_default.warn("Unexpected status:", changeInfo.status);
	}
	chrome_tabs_onRemoved(tabId) {
		if (!this.openTabs.delete(tabId)) logger_default.debug("Trying to remove unknown tab:", tabId);
		if (this.activeTab.tabId === tabId) this.activeTab.updateActiveTab({
			tabId: -1,
			windowId: -1
		});
	}
	chrome_tabs_onActivated(activeInfo) {
		const now = Date.now();
		const { tabId, windowId } = activeInfo;
		this.activeTab.updateActiveTab({
			tabId,
			windowId,
			now
		});
		const entry = this.openTabs.get(tabId);
		if (entry?.status === "complete") this._initAllOptionalFields(tabId, entry, now);
	}
	chrome_webRequest_onBeforeRequest(details) {
		const { incognito, type, tabId, originUrl } = details;
		if (incognito || tabId === -1 || type !== "main_frame") return;
		if (this.enableFirefoxAndroidQuirks) {
			const oldEntry = this.openTabs.get(tabId);
			if (oldEntry?._unverifiedOpener__FirefoxAndroid__) {
				let { _unverifiedOpener__FirefoxAndroid__: candidate, ...entry } = oldEntry;
				entry.lastUpdatedAt = Date.now();
				if (!entry.openedFrom && entry.status === "created" && this.activeTab.tabId !== tabId && originUrl === candidate.expectedOriginUrl) {
					entry.openedFrom = { ...candidate.openedFrom };
					logger_default.debug("confirmed openedFrom:", oldEntry, "->", entry);
				} else logger_default.debug("rejected openedFrom:", oldEntry, "->", entry);
				this.openTabs.set(tabId, entry);
			}
		}
	}
	chrome_webRequest_onBeforeRedirect(details) {
		if (details.url && details.ip) this._cacheDnsResolution(details.url, details.ip);
		if (details.type !== "main_frame") return;
		if (details.statusCode === 0) return;
		const { tabId, statusCode, url: from, redirectUrl: to } = details;
		logger_default.debug("Detected redirect:", from, "->", to, "with tabId", tabId);
		const oldEntry = this.openTabs.get(tabId);
		const entry = {
			...oldEntry,
			status: "redirecting",
			lastUpdatedAt: Date.now()
		};
		if (oldEntry && oldEntry.status === "created" && oldEntry.openedFrom) entry.openedFrom = { ...oldEntry.openedFrom };
		const thisRedirect = {
			from,
			to,
			statusCode
		};
		if (!entry.pendingRedirects) entry.pendingRedirects = [thisRedirect];
		else if (entry.pendingRedirects.length < this.maxRedirects) entry.pendingRedirects = [...entry.pendingRedirects, thisRedirect];
		else if (entry.pendingRedirects[entry.pendingRedirects.length - 1].to !== "...") {
			logger_default.warn("Break exceptionally long redirect chain (redirect loop?):", entry.pendingRedirects, "-->", thisRedirect, "(destination will be replaced by \"...\")");
			thisRedirect.to = "...";
			entry.pendingRedirects = [...entry.pendingRedirects, thisRedirect];
		} else entry.pendingRedirects = [...entry.pendingRedirects];
		this.openTabs.set(tabId, entry);
	}
	chrome_webRequest_onResponseStarted(details) {
		if (details.url && details.ip) this._cacheDnsResolution(details.url, details.ip);
	}
	chrome_webRequest_onCompleted(details) {
		if (this.activeTab.tabId === details.tabId) {
			const entry = this.openTabs.get(details.tabId);
			if (entry && isRealPage(entry.url)) {
				let contentLength = 0;
				for (const { name, value } of details.responseHeaders) if (name.toLowerCase() === "content-length") {
					contentLength = Number(value) || 0;
					break;
				}
				if (contentLength > 1024) this.activityEstimator.dynamicLoadDetected(entry.url);
			}
		}
	}
	chrome_webRequest_onAuthRequired(details) {
		const { tabId, url } = details;
		const entry = this.openTabs.get(tabId);
		if (entry?.url === url) this.openTabs.set(tabId, {
			entry,
			visibility: "private",
			pageId: ++this._pageIdGenerator,
			lastUpdatedAt: Date.now()
		});
		this.newPageApprover.markAsPrivate(url).catch((e) => {
			logger_default.warn("Failed to mark page as private:", url, e);
		});
	}
	chrome_webNavigation_onCreatedNavigationTarget(details) {
		if (details.sourceFrameId !== 0) return;
		const { tabId, sourceTabId, windowId } = details;
		const openedFrom = this.openTabs.get(sourceTabId);
		if (!openedFrom) return;
		const entry = {
			status: "created",
			...this.openTabs.get(tabId),
			pageId: ++this._pageIdGenerator,
			windowId,
			lastUpdatedAt: Date.now(),
			openedFrom: { ...openedFrom }
		};
		this.openTabs.set(tabId, entry);
	}
	chrome_webNavigation_onBeforeNavigate(details) {
		if (details.frameId !== 0) return;
		const { tabId, url } = details;
		const oldEntry = this.openTabs.get(tabId);
		if (oldEntry && oldEntry.url !== url) this.openTabs.set(tabId, {
			...oldEntry,
			pageId: ++this._pageIdGenerator,
			lastUpdatedAt: Date.now()
		});
	}
	chrome_webNavigation_onCommitted(details) {
		const { tabId, frameId, url } = details;
		if (frameId !== 0) return;
		let entry = this.openTabs.get(tabId);
		if (!entry) {
			logger_default.debug("Skipping navigation to", url, "in private tab with", tabId);
			return;
		}
		const now = Date.now();
		entry = {
			...entry,
			lastUpdated: now,
			_previous_webNavigation_onCommitted: {
				url,
				lastUpdated: now
			}
		};
		const isBackOrForward = details.transitionQualifiers?.includes("forward_back");
		let forgetOpenedFrom = details.transitionType !== "link";
		if (isBackOrForward) forgetOpenedFrom = true;
		if (forgetOpenedFrom && (entry.previousUrl || entry.openedFrom)) {
			const { previousUrl, openedFrom, search, ...newEntry } = { ...entry };
			entry = newEntry;
		}
		this.openTabs.set(tabId, entry);
		this._onWebNavigation(tabId, url, entry, { isHistoryNavigation: false });
	}
	chrome_webNavigation_onHistoryStateUpdated(details) {
		const { tabId, frameId, parentFrameId, url } = details;
		if (frameId !== 0) return;
		if (parentFrameId !== -1) return;
		const oldEntry = this.openTabs.get(tabId);
		if (!oldEntry) {
			logger_default.debug("Skipping history navigation to", url, "in private tab with", tabId);
			return;
		}
		const { _previous_webNavigation_onCommitted, ...entry } = oldEntry;
		this.openTabs.set(tabId, {
			...entry,
			pageLoadMethod: "history-navigation",
			status: "after-history-state-update",
			lastUpdatedAt: Date.now(),
			pageId: ++this._pageIdGenerator
		});
		if (_previous_webNavigation_onCommitted?.url === url) logger_default.debug("Skipping page event for redundant history navigation to", url, "in tab", tabId);
		else this._onWebNavigation(tabId, url, entry, { isHistoryNavigation: true });
	}
	_onWebNavigation(tabId, url, entry, { isHistoryNavigation }) {
		if (!isRealPage(url)) return;
		if (url !== entry.url) {
			logger_default.warn("webNavigation reports a navigation in tab", tabId, "to", url, "This is inconsistent with the openTabs URL:", entry.url);
			return;
		}
		if (this.dnsResolver.isPrivateURL(url)) {
			logger_default.debug("Skipping navigation to private URL:", url);
			return;
		}
		logger_default.debug("Detected safe page navigation to URL:", url, "in tab", tabId);
		const pageEvent = {
			type: "safe-page-navigation",
			url,
			isHistoryNavigation,
			tabId
		};
		if (entry.redirects) pageEvent.redirects = entry.redirects.map(({ from, to, statusCode }) => ({
			from: this.dnsResolver.isPrivateURL(from) ? null : from,
			to: this.dnsResolver.isPrivateURL(to) ? null : to,
			statusCode
		}));
		if (entry.previousUrl) pageEvent.previousUrl = this.dnsResolver.isPrivateURL(entry.previousUrl) ? null : entry.previousUrl;
		this.notifyObservers(pageEvent);
	}
	chrome_windows_onRemoved(windowId) {
		if (windowId !== -1) this.activeTab.removeWindow(windowId);
	}
	/**
	* This API makes more sense on Desktop than on Mobile:
	* - Unavailabe on Firefox for Android.
	* - Safari on iOS (https://developer.mozilla.org/en-US/docs/Mozilla/Add-ons/WebExtensions/API/windows/onFocusChanged):
	*   > Fires when toggling in or out of private browsing mode, leaving Safari
	*   > to the home screen, and returning to Safari from the home screen.
	*/
	chrome_windows_onFocusChanged(windowId) {
		if (windowId !== -1) this.activeTab.focusWindow(windowId);
	}
	/**
	* Tabs can be closed at any point. This function detects such cases
	* and warns as a side-effect. Thus, it should be used only in situations
	* where it is possible, but unexpected that the tab is gone.
	*/
	_tabVanished(tabId, pageId, ...details) {
		const entry = this.openTabs.get(tabId);
		if (!entry) {
			logger_default.debug("Tab", tabId, "closed:", ...details);
			return true;
		}
		if (entry.pageId !== pageId) {
			logger_default.debug("Tab", tabId, "changed pageId:", ...details);
			return true;
		}
		return false;
	}
	_cacheDnsResolution(url, ip) {
		if (this.dnsResolver.cacheDnsResolution({
			url,
			ip
		}) && this.sessionStore.isReady()) this.sessionStore.set("dnsResolver", this.dnsResolver.serialize());
	}
	_initVisibility(tabId, entry, now) {
		if (!entry.visibility) {
			const { visibility, search } = this._checkPageVisibility(entry.url);
			entry.visibility = visibility;
			if (visibility !== "private" && search) entry.search = {
				category: search.category,
				query: search.query,
				depth: 0,
				lastUpdatedAt: now
			};
		}
	}
	_initAllOptionalFields(tabId, entry, now) {
		const { pageId } = entry;
		this._initVisibility(tabId, entry, now);
		this._tryAnalyzePageStructure(tabId, entry, now);
		this._tryLanguageDetection(tabId, pageId, entry);
	}
	_tryLanguageDetection(tabId, pageId, entry) {
		this._setLazyVar("language", entry, tabId, {
			precond: () => entry.visibility !== "private" && isRealPage(entry.url),
			init: () => this._detectLanguage(tabId, pageId, entry)
		});
	}
	/**
	* There are three types of results:
	*
	* 1) If it was successful in detecting a language, it returns something
	*    'en', 'de', 'fr'.
	* 2) If the language could not be detected by the API or belonged to a
	*    group that is considered too small to be safe, '--' will be returned.
	* 3) If the language detection was skipped, it returns an empty string ('')
	*/
	async _detectLanguage(tabId, pageId, entry) {
		if (!this.chrome?.tabs?.detectLanguage) return "";
		if (entry.pageStructure) try {
			if ((await this._awaitLazyVar(entry.pageStructure))?.noindex) {
				logger_default.debug("No need to detect the language, since the page is private");
				return "";
			}
		} catch (e) {
			logger_default.warn("Unexpected error while waiting for the page structure:", e);
			return "";
		}
		if (this._tabVanished(tabId, pageId, "before detectLanguage")) return CANCEL_LAZY_VAR;
		try {
			const lang = await this.chrome.tabs.detectLanguage(tabId);
			const normalizedLang = this._normalizeDetectedLanguage(lang);
			logger_default.debug("detected language for tabID", tabId, ":", lang, "->", normalizedLang);
			return normalizedLang;
		} catch (e) {
			if (this._tabVanished(tabId, pageId, "during detectLanguage")) return CANCEL_LAZY_VAR;
			if (e.message === "Cannot determine language") {
				logger_default.debug("API to detect language is not available", e);
				return "";
			}
			logger_default.warn("Unexpected error", e);
			return "";
		}
	}
	/**
	* Takes the output of chrome.tabs.detectLanguage and maps it to well-known values.
	* If possible, avoid browser-specific differences (e.g. "de" on Firefox/Chrome vs
	* "de-DE" on Safari). Also, hide languages that we did not expect, or that are
	* expected to be relatively rare.
	*/
	_normalizeDetectedLanguage(lang) {
		this.langStats[lang] = (this.langStats[lang] || 0) + 1;
		const cleanedLang = split0(lang, "-");
		switch (cleanedLang) {
			case "af":
			case "ar":
			case "ca":
			case "cs":
			case "da":
			case "de":
			case "el":
			case "en":
			case "es":
			case "fa":
			case "fi":
			case "fr":
			case "hi":
			case "hr":
			case "hu":
			case "id":
			case "it":
			case "ja":
			case "ko":
			case "nl":
			case "no":
			case "pl":
			case "pt":
			case "ro":
			case "ru":
			case "sk":
			case "sl":
			case "sq":
			case "sr":
			case "sv":
			case "th":
			case "tr":
			case "uk":
			case "vi":
			case "zh": return cleanedLang;
			case "und": return "--";
			case "la":
			case "fy":
			case "yi":
			case "iw":
			case "he": return "--";
		}
		if (this.langStats[lang] === 1) logger_default.info("Unexpected language:", lang);
		return "--";
	}
	_tryAnalyzePageStructure(tabId, entry) {
		const { pageId } = entry;
		this._setLazyVar("pageStructure", entry, tabId, {
			precond: () => entry.status === "complete" && entry.visibility !== "private" && isRealPage(entry.url),
			init: () => this._analyzePageStructure(tabId, pageId)
		});
	}
	async _analyzePageStructure(tabId, pageId) {
		if (this._tabVanished(tabId, pageId, "before analyzePageStructure")) return CANCEL_LAZY_VAR;
		try {
			const { result } = (await this.chrome.scripting.executeScript({
				target: { tabId },
				func: analyzePageStructure
			}))[0];
			if (this._tabVanished(tabId, pageId, "after analyzePageStructure")) {
				logger_default.warn("analyzePageStructure completed but the page has changed. Ignoring results:", result);
				return CANCEL_LAZY_VAR;
			}
			logger_default.debug("Page structure:", result);
			return {
				...result,
				lastUpdatedAt: Date.now()
			};
		} catch (e) {
			const msg = e.message;
			if (msg === "The extensions gallery cannot be scripted.") return {
				noindex: true,
				details: msg
			};
			if (this._tabVanished(tabId, pageId, "during analyzePageStructure")) return CANCEL_LAZY_VAR;
			if (msg.startsWith("Frame with ID") && msg.endsWith("is showing error page")) return {
				noindex: true,
				details: "Page failed to load"
			};
			logger_default.error("Unable to extract structural information", e);
			return {
				error: true,
				details: `Unable to run analyzePageStructure script (reason: ${e})`,
				noindex: true
			};
		}
	}
	_checkPageVisibility(url) {
		if (!isRealPage(url)) return {
			visibility: "private",
			reason: "not an HTTP page"
		};
		const { category, query } = this.urlAnalyzer.parseSearchLinks(url);
		if (category && query) return {
			visibility: "public",
			reason: "indexed by search engine",
			search: {
				category: category.startsWith("search-") ? category.slice(7) : category,
				query
			}
		};
		if (this.dnsResolver.isPrivateURL(url)) return {
			visibility: "private",
			reason: "private network"
		};
		return {
			visibility: "unknown",
			reason: "further checks are needed"
		};
	}
	_setLazyVar(field, entry, tabId, { init: asyncInit, precond = () => true }) {
		if (!entry[field] && precond()) {
			entry[field] = { _pending: asyncInit() };
			entry[field]._pending.then((result) => {
				const now = Date.now();
				entry[field].result = result;
				entry[field].resolvedAt = now;
				entry.lastUpdatedAt = now;
				if (this.openTabs.get(tabId)?.[field] === entry[field]) {
					const entry_ = this.openTabs.get(tabId);
					entry_[field] = result;
					entry_.lastUpdatedAt = now;
				}
				entry[field] = result;
				this._onTabChanged(tabId);
				this.notifyObservers({
					type: "lazy-init",
					field,
					tabId
				});
			}).catch((e) => {
				logger_default.error(`Internal error while initializing field: "${field}" (it is a bug in the code)`, e);
			});
		}
	}
	_tryUnwrapLazyVar(value, callbackIfPresent) {
		if (!isNil(value) && value !== CANCEL_LAZY_VAR) {
			if (!value._pending) callbackIfPresent(value);
			else if (!isNil(value.result) && value.result !== CANCEL_LAZY_VAR) callbackIfPresent(value.result);
		}
	}
	async _awaitLazyVar(value) {
		if (!value) throw new Error("Lazy var not found (cannot await a lazy var before it has been initialized)");
		await value._pending;
		return value._pending ? value.result : value;
	}
	/**
	* Listeners defined in this class follow a naming convention, for instance:
	* - "chrome_tabs_onCreated" (listener for chrome.tabs.onCreated)
	* - "chrome_webNavigation_onCommitted" (chrome.webNavigation.onCommitted)
	*
	* Example output:
	* [
	*   { method: 'chrome_tabs_onCreated', api: 'tabs', type: 'onCreated' },
	*   { method: 'chrome_webNavigation_onCommitted', api: 'webNavigation', type: 'onCommitted' },
	* ]
	*/
	static describeListeners() {
		const listeners = Object.getOwnPropertyNames(Pages);
		return [
			"webRequest",
			"webNavigation",
			"tabs",
			"windows"
		].flatMap((api) => {
			const prefix = `chrome_${api}_`;
			return listeners.filter((method) => method.startsWith(prefix)).map((method) => ({
				method,
				api,
				type: method.slice(prefix.length)
			}));
		});
	}
	async selfChecks(check = new SelfCheck()) {
		const myChecks = (async () => {
			if (this.isActive) if (this.sessionStore.isReady()) {
				const expectedSession = this._serializeFullSession();
				const detectedPromises = flattenObject(expectedSession).filter(({ path }) => path.includes("_pending") || path.includes(CANCEL_LAZY_VAR._tag));
				if (detectedPromises.length > 0) check.fail("Promises must not be serialized", { detectedPromises });
				else {
					const realSession = this.sessionStore.getEntries();
					if (equalityCanBeProven(expectedSession, realSession)) check.pass("Session in sync", {
						expectedSession,
						realSession
					});
					else check.warn("Session may be out to sync", {
						expectedSession,
						realSession
					});
				}
			} else check.warn("initialization of sessionStore not finished");
			const tabsWithMissingPageIds = [];
			const pageIds = /* @__PURE__ */ new Set();
			const pageIdClashes = [];
			for (const [tabId, entry] of this.openTabs.entries()) {
				if (!entry.pageId) tabsWithMissingPageIds.push([tabId, entry]);
				if (pageIds.has(tabId)) pageIdClashes.push([tabId, entry]);
				else pageIds.add(tabId);
			}
			if (tabsWithMissingPageIds.length > 0) check.fail("Found entry without page ids", Object.fromEntries(tabsWithMissingPageIds));
			if (pageIdClashes.length > 0) check.warn("Found a pageId clash", Object.fromEntries(pageIdClashes));
			if (this.activeTab.tabId !== -1 && this.activeTab.windowId) {
				const entry = this.openTabs.get(this.activeTab.tabId);
				if (entry?.windowId !== this.activeTab.windowId) check.fail("mismatch between the activeTab and the openTabs", {
					activeTab: {
						tabId: this.activeTab.tabId,
						windowId: this.activeTab.windowId
					},
					entry: entry ? { ...entry } : `<tab not found>`
				});
			}
			for (const [tabId, entry] of this.openTabs.entries()) {
				if (isNil(entry.search)) continue;
				const fail = (msg) => {
					check.fail(`inconsistency: ${msg}`, {
						tabId,
						entry: { ...entry }
					});
				};
				if (isRealPage(entry.ref) && isRealPage(entry.url)) {
					if (entry.search.depth === 1 && this.urlAnalyzer.parseSearchLinks(entry.ref).category !== entry.search.category && sameHostname(entry.ref, entry.url)) fail("depth=1 search landing must not include internal page navigation");
					else if (entry.search.depth === 2 && !sameHostname(entry.ref, entry.url)) fail("depth=2 search landings must stay on the same domains");
				}
				if (![
					0,
					1,
					2
				].includes(entry.search.depth)) fail("depth must be in { 0, 1, 2 }");
			}
		})();
		await Promise.all([
			myChecks,
			eventListenerQueue.selfChecks(check.for("eventListenerQueue")),
			this.activeTab.selfChecks(check.for("activeTab")),
			this.activityEstimator.selfChecks(check.for("activityEstimator"))
		]);
		return check;
	}
};
var eventListenerQueue = new EventListenerQueue({
	connectTimeoutInMs: 1e3,
	maxBufferLength: 1024,
	listeners: Pages.describeListeners()
});
//#endregion
export { Pages as default };
