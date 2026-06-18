import SearchParams from "../../../../url-parser/dist/esm/url-search-params.js";
import "../../../../url-parser/dist/esm/index.js";
import { StaticDataView, sizeOfASCII, sizeOfBool, sizeOfByte } from "../data-view.js";
import Config from "../config.js";
import { EventEmitter } from "../events.js";
import { adsAndTrackingLists, adsLists, fetchLists, fetchResources, fullLists } from "../fetch.js";
import Request from "../request.js";
import { normalizeSelector } from "../filters/cosmetic.js";
import { block } from "../filters/dsl.js";
import Preprocessor, { Env } from "../preprocessor.js";
import { FilterType, parseFilters } from "../lists.js";
import Resources from "../resources.js";
import CosmeticFilterBucket from "./bucket/cosmetic.js";
import NetworkFilterBucket from "./bucket/network.js";
import HTMLBucket from "./bucket/html.js";
import { Metadata } from "./metadata.js";
import PreprocessorBucket from "./bucket/preprocessor.js";
import { binaryMerge, legacyMerge } from "./merger.js";
/*!
* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/
function findApplicableHideException(filters) {
	if (filters.length === 0) return;
	let hideFilter;
	let currentScore = 0;
	for (const filter of filters) {
		const score = (filter.isImportant() ? 4 : 0) | (filter.isException() ? 1 : 2);
		if (score >= currentScore) {
			currentScore = score;
			hideFilter = filter;
		}
	}
	if (hideFilter === void 0) return;
	if (hideFilter.isException() === false) return;
	return hideFilter;
}
var FilterEngine = class extends EventEmitter {
	static fromCached(init, caching) {
		if (caching === void 0) return init();
		const { path, read, write } = caching;
		return read(path).then((buffer) => this.deserialize(buffer)).catch(() => init().then((engine) => write(path, engine.serialize()).then(() => engine)));
	}
	static empty(config = {}) {
		return new this({ config });
	}
	/**
	* Create an instance of `FiltersEngine` (or subclass like `ElectronBlocker`,
	* etc.), from the list of subscriptions provided as argument (e.g.:
	* EasyList).
	*
	* Lists are fetched using the instance of `fetch` provided as a first
	* argument. Optionally resources.txt and config can be provided.
	*/
	static fromLists(fetch, urls, config = {}, caching) {
		return this.fromCached(() => {
			const listsPromises = fetchLists(fetch, urls);
			const resourcesPromise = fetchResources(fetch);
			return Promise.all([listsPromises, resourcesPromise]).then(([lists, resources]) => {
				const engine = this.parse(lists.join("\n"), config);
				if (resources !== void 0) engine.updateResources(resources, "" + resources.length);
				return engine;
			});
		}, caching);
	}
	/**
	* Initialize blocker of *ads only*.
	*
	* Attempt to initialize a blocking engine using a pre-built version served
	* from Ghostery's CDN. If this fails (e.g.: if no pre-built engine is available
	* for this version of the library), then falls-back to using `fromLists(...)`
	* method with the same subscriptions.
	*/
	static fromPrebuiltAdsOnly(fetchImpl = fetch, caching) {
		return this.fromLists(fetchImpl, adsLists, {}, caching);
	}
	/**
	* Same as `fromPrebuiltAdsOnly(...)` but also contains rules to block
	* tracking (i.e.: using extra lists such as EasyPrivacy and more).
	*/
	static fromPrebuiltAdsAndTracking(fetchImpl = fetch, caching) {
		return this.fromLists(fetchImpl, adsAndTrackingLists, {}, caching);
	}
	/**
	* Same as `fromPrebuiltAdsAndTracking(...)` but also contains annoyances
	* rules to block things like cookie notices.
	*/
	static fromPrebuiltFull(fetchImpl = fetch, caching) {
		return this.fromLists(fetchImpl, fullLists, {}, caching);
	}
	static fromTrackerDB(rawJsonDump, options = {}) {
		const config = new Config(options);
		const metadata = new Metadata(rawJsonDump);
		const filters = [];
		for (const pattern of metadata.getPatterns()) filters.push(...pattern.filters);
		const engine = this.parse(filters.join("\n"), config);
		engine.metadata = metadata;
		return engine;
	}
	/**
	* Merges compatible engines into one.
	*
	* This action references objects from the source engines, including
	* network filters, cosmetic filters, preprocessors, metadata, and lists.
	* These objects are not deep-copied, so modifying them directly can have
	* unintended side effects.
	* However, resources are deep-copied from the first engine.
	*
	* Optionally, you can specify a second parameter to skip merging specific
	* resources or override the resulting engine config. If resource merging is
	* skipped, the resulting engine will be assigned empty resources.
	*
	* Set `useBinaryMerge` to use the byte-level merge path. This method is
	* faster for large engines, but it has stricter requirements: source engines
	* must not be built with `debug: true`, their compression settings must
	* match, and the resulting engine cannot enable debug or compression through
	* `overrideConfig`.
	*
	* When using `useBinaryMerge`, you can pass `hashFunc` to deduplicate
	* serialized filters. Prefer a collision-resistant bigint or string hash for
	* large merges; the built-in fallback is intended for convenience and does
	* not provide strict collision-proof deduplication.
	*
	* Call `updateEnv` on the merged engine before use when preprocessor bindings
	* should affect which filters are active.
	*
	* Binary merge skips buckets disabled by target load flags. It does not skip
	* buckets for `loadExtendedSelectors`, `enableInMemoryCache`, or
	* `enableOptimizations`, which do not represent whole filter categories.
	*/
	static merge(engines, opts = {}) {
		if (opts.useBinaryMerge === true) return binaryMerge(this, engines, opts);
		return legacyMerge(this, engines, opts);
	}
	static parse(filters, options = {}) {
		const config = new Config(options);
		return new this({
			...parseFilters(filters, config),
			config
		});
	}
	static deserialize(serialized) {
		const buffer = StaticDataView.fromUint8Array(serialized, { enableCompression: false });
		const serializedEngineVersion = buffer.getUint16();
		if (869 !== serializedEngineVersion) throw new Error(`serialized engine version mismatch, expected 869 but got ${serializedEngineVersion}`);
		const config = Config.deserialize(buffer);
		if (config.enableCompression) buffer.enableCompression();
		if (config.integrityCheck) {
			const currentPos = buffer.pos;
			buffer.pos = serialized.length - 4;
			const checksum = buffer.checksum();
			const expected = buffer.getUint32();
			if (checksum !== expected) throw new Error(`serialized engine checksum mismatch, expected ${expected} but got ${checksum}`);
			buffer.pos = currentPos;
		}
		const engine = new this({ config });
		engine.resources = Resources.deserialize(buffer);
		const lists = /* @__PURE__ */ new Map();
		const numberOfLists = buffer.getUint16();
		for (let i = 0; i < numberOfLists; i += 1) lists.set(buffer.getASCII(), buffer.getASCII());
		engine.lists = lists;
		engine.preprocessors = PreprocessorBucket.deserialize(buffer);
		engine.importants = NetworkFilterBucket.deserialize(buffer, config);
		engine.redirects = NetworkFilterBucket.deserialize(buffer, config);
		engine.removeparams = NetworkFilterBucket.deserialize(buffer, config);
		engine.filters = NetworkFilterBucket.deserialize(buffer, config);
		engine.exceptions = NetworkFilterBucket.deserialize(buffer, config);
		engine.csp = NetworkFilterBucket.deserialize(buffer, config);
		engine.cosmetics = CosmeticFilterBucket.deserialize(buffer, config);
		engine.hideExceptions = NetworkFilterBucket.deserialize(buffer, config);
		engine.htmlFilters = HTMLBucket.deserialize(buffer, config);
		if (buffer.getBool()) engine.metadata = Metadata.deserialize(buffer);
		buffer.seekZero();
		return engine;
	}
	constructor({ cosmeticFilters = [], networkFilters = [], preprocessors = [], config = new Config(), lists = /* @__PURE__ */ new Map() } = {}) {
		super();
		this.config = new Config(config);
		this.lists = lists;
		this.preprocessors = new PreprocessorBucket({});
		this.csp = new NetworkFilterBucket({ config: this.config });
		this.hideExceptions = new NetworkFilterBucket({ config: this.config });
		this.exceptions = new NetworkFilterBucket({ config: this.config });
		this.importants = new NetworkFilterBucket({ config: this.config });
		this.redirects = new NetworkFilterBucket({ config: this.config });
		this.removeparams = new NetworkFilterBucket({ config: this.config });
		this.filters = new NetworkFilterBucket({ config: this.config });
		this.cosmetics = new CosmeticFilterBucket({ config: this.config });
		this.htmlFilters = new HTMLBucket({ config: this.config });
		this.resources = new Resources();
		if (networkFilters.length !== 0 || cosmeticFilters.length !== 0) this.update({
			newCosmeticFilters: cosmeticFilters,
			newNetworkFilters: networkFilters,
			newPreprocessors: preprocessors
		});
	}
	isFilterExcluded(filter) {
		return this.preprocessors.isFilterExcluded(filter);
	}
	updateEnv(env) {
		this.preprocessors.updateEnv(env);
	}
	/**
	* Estimate the number of bytes needed to serialize this instance of
	* `FiltersEngine` using the `serialize(...)` method. It is used internally
	* by `serialize(...)` to allocate a buffer of the right size and you should
	* not have to call it yourself most of the time.
	*
	* There are cases where we cannot estimate statically the exact size of the
	* resulting buffer (due to alignement which needs to be performed); this
	* method will return a safe estimate which will always be at least equal to
	* the real number of bytes needed, or bigger (usually of a few bytes only:
	* ~20 bytes is to be expected).
	*/
	getSerializedSize() {
		let estimatedSize = sizeOfByte() * 2 + this.config.getSerializedSize() + this.resources.getSerializedSize() + this.preprocessors.getSerializedSize() + this.filters.getSerializedSize() + this.exceptions.getSerializedSize() + this.importants.getSerializedSize() + this.redirects.getSerializedSize() + this.removeparams.getSerializedSize() + this.csp.getSerializedSize() + this.cosmetics.getSerializedSize() + this.hideExceptions.getSerializedSize() + this.htmlFilters.getSerializedSize() + 4;
		estimatedSize += sizeOfByte() * 2;
		for (const [name, checksum] of this.lists) estimatedSize += sizeOfASCII(name) + sizeOfASCII(checksum);
		estimatedSize += sizeOfBool();
		if (this.metadata !== void 0) estimatedSize += this.metadata.getSerializedSize();
		return estimatedSize;
	}
	/**
	* Creates a binary representation of the full engine. It can be stored
	* on-disk for faster loading of the adblocker. The `deserialize` static
	* method of Engine can be used to restore the engine.
	*/
	serialize(array) {
		const buffer = StaticDataView.fromUint8Array(array || new Uint8Array(this.getSerializedSize()), this.config);
		buffer.pushUint16(869);
		this.config.serialize(buffer);
		this.resources.serialize(buffer);
		buffer.pushUint16(this.lists.size);
		for (const [name, value] of Array.from(this.lists.entries()).sort()) {
			buffer.pushASCII(name);
			buffer.pushASCII(value);
		}
		this.preprocessors.serialize(buffer);
		this.importants.serialize(buffer);
		this.redirects.serialize(buffer);
		this.removeparams.serialize(buffer);
		this.filters.serialize(buffer);
		this.exceptions.serialize(buffer);
		this.csp.serialize(buffer);
		this.cosmetics.serialize(buffer);
		this.hideExceptions.serialize(buffer);
		this.htmlFilters.serialize(buffer);
		buffer.pushBool(this.metadata !== void 0);
		if (this.metadata !== void 0) this.metadata.serialize(buffer);
		if (this.config.integrityCheck) buffer.pushUint32(buffer.checksum());
		return buffer.subarray();
	}
	/**
	* Update engine with new filters or resources.
	*/
	loadedLists() {
		return Array.from(this.lists.keys());
	}
	hasList(name, checksum) {
		return this.lists.has(name) && this.lists.get(name) === checksum;
	}
	/**
	* Update engine with `resources.txt` content.
	*/
	updateResources(data, checksum) {
		if (this.resources.checksum === checksum) return false;
		this.resources = Resources.parse(data, { checksum });
		return true;
	}
	getFilters() {
		const cosmeticFilters = this.cosmetics.getFilters();
		const networkFilters = [
			...this.filters.getFilters(),
			...this.exceptions.getFilters(),
			...this.importants.getFilters(),
			...this.redirects.getFilters(),
			...this.csp.getFilters(),
			...this.hideExceptions.getFilters(),
			...this.removeparams.getFilters()
		];
		for (const filter of this.htmlFilters.getFilters()) if (filter.isNetworkFilter()) networkFilters.push(filter);
		else if (filter.isCosmeticFilter()) cosmeticFilters.push(filter);
		return {
			cosmeticFilters,
			networkFilters
		};
	}
	/**
	* Update engine with new filters as well as optionally removed filters.
	*/
	update({ newNetworkFilters = [], newCosmeticFilters = [], newPreprocessors = [], removedCosmeticFilters = [], removedNetworkFilters = [], removedPreprocessors = [] }, env = new Env()) {
		let updated = false;
		if (this.config.loadPreprocessors && (newPreprocessors.length !== 0 || removedPreprocessors.length !== 0)) {
			updated = true;
			this.preprocessors.update({
				added: newPreprocessors,
				removed: removedPreprocessors
			}, env);
		}
		const htmlFilters = [];
		if (this.config.loadCosmeticFilters && (newCosmeticFilters.length !== 0 || removedCosmeticFilters.length !== 0)) {
			updated = true;
			const cosmeticFitlers = [];
			for (const filter of newCosmeticFilters) if (filter.isHtmlFiltering()) htmlFilters.push(filter);
			else cosmeticFitlers.push(filter);
			this.cosmetics.update(cosmeticFitlers, removedCosmeticFilters.length === 0 ? void 0 : new Set(removedCosmeticFilters), this.config);
		}
		if (this.config.loadNetworkFilters && (newNetworkFilters.length !== 0 || removedNetworkFilters.length !== 0)) {
			updated = true;
			const filters = [];
			const csp = [];
			const exceptions = [];
			const importants = [];
			const redirects = [];
			const removeparams = [];
			const hideExceptions = [];
			for (const filter of newNetworkFilters) if (filter.isCSP()) csp.push(filter);
			else if (filter.isHtmlFilteringRule()) htmlFilters.push(filter);
			else if (filter.isGenericHide() || filter.isSpecificHide()) hideExceptions.push(filter);
			else if (filter.isException()) if (filter.isRemoveParam()) removeparams.push(filter);
			else exceptions.push(filter);
			else if (filter.isImportant()) importants.push(filter);
			else if (filter.isRedirect()) redirects.push(filter);
			else if (filter.isRemoveParam()) removeparams.push(filter);
			else filters.push(filter);
			const removedNetworkFiltersSet = removedNetworkFilters.length === 0 ? void 0 : new Set(removedNetworkFilters);
			this.importants.update(importants, removedNetworkFiltersSet);
			this.redirects.update(redirects, removedNetworkFiltersSet);
			this.removeparams.update(removeparams, removedNetworkFiltersSet);
			this.filters.update(filters, removedNetworkFiltersSet);
			if (this.config.loadExceptionFilters === true) this.exceptions.update(exceptions, removedNetworkFiltersSet);
			if (this.config.loadCSPFilters === true) this.csp.update(csp, removedNetworkFiltersSet);
			this.hideExceptions.update(hideExceptions, removedNetworkFiltersSet);
		}
		if (this.config.enableHtmlFiltering && (htmlFilters.length !== 0 || removedNetworkFilters.length !== 0 || removedCosmeticFilters.length !== 0)) {
			const removeFilters = new Set([...removedNetworkFilters, ...removedCosmeticFilters]);
			this.htmlFilters.update(htmlFilters, removeFilters);
		}
		return updated;
	}
	updateFromDiff({ added, removed, preprocessors }, env) {
		const newCosmeticFilters = [];
		const newNetworkFilters = [];
		const newPreprocessors = [];
		const removedCosmeticFilters = [];
		const removedNetworkFilters = [];
		const removedPreprocessors = [];
		if (removed !== void 0 && removed.length !== 0) {
			const { networkFilters, cosmeticFilters } = parseFilters(removed.join("\n"), this.config);
			Array.prototype.push.apply(removedCosmeticFilters, cosmeticFilters);
			Array.prototype.push.apply(removedNetworkFilters, networkFilters);
		}
		if (added !== void 0 && added.length !== 0) {
			const { networkFilters, cosmeticFilters } = parseFilters(added.join("\n"), this.config);
			Array.prototype.push.apply(newCosmeticFilters, cosmeticFilters);
			Array.prototype.push.apply(newNetworkFilters, networkFilters);
		}
		if (preprocessors !== void 0) for (const [condition, details] of Object.entries(preprocessors)) {
			if (details.removed !== void 0 && details.removed.length !== 0) {
				const { networkFilters, cosmeticFilters } = parseFilters(details.removed.join("\n"), this.config);
				const filterIDs = new Set([].concat(cosmeticFilters.map((filter) => filter.getId())).concat(networkFilters.map((filter) => filter.getId())));
				removedPreprocessors.push(new Preprocessor({
					condition,
					filterIDs
				}));
			}
			if (details.added !== void 0 && details.added.length !== 0) {
				const { networkFilters, cosmeticFilters } = parseFilters(details.added.join("\n"), this.config);
				const filterIDs = new Set([].concat(cosmeticFilters.map((filter) => filter.getId())).concat(networkFilters.map((filter) => filter.getId())));
				newPreprocessors.push(new Preprocessor({
					condition,
					filterIDs
				}));
			}
		}
		return this.update({
			newCosmeticFilters,
			newNetworkFilters,
			newPreprocessors,
			removedCosmeticFilters: removedCosmeticFilters.map((f) => f.getId()),
			removedNetworkFilters: removedNetworkFilters.map((f) => f.getId()),
			removedPreprocessors
		}, env);
	}
	/**
	* Return a list of HTML filtering rules.
	*/
	getHtmlFilters(request) {
		const htmlSelectors = [];
		if (this.config.enableHtmlFiltering === false) return htmlSelectors;
		const { networkFilters, exceptions, cosmeticFilters, unhides } = this.htmlFilters.getHTMLFilters(request, this.isFilterExcluded.bind(this));
		if (cosmeticFilters.length !== 0) {
			const unhideMap = new Map(unhides.map((unhide) => [unhide.getSelector(), unhide]));
			for (const filter of cosmeticFilters) {
				const extended = filter.getExtendedSelector();
				if (extended === void 0) continue;
				const unhide = unhideMap.get(filter.getSelector());
				if (unhide === void 0) htmlSelectors.push(extended);
				this.emit("filter-matched", {
					filter,
					exception: unhide
				}, {
					request,
					filterType: FilterType.COSMETIC
				});
			}
		}
		if (networkFilters.length !== 0) {
			const exceptionsMap = /* @__PURE__ */ new Map();
			let replaceDisabledException;
			for (const exception of exceptions) {
				const optionValue = exception.optionValue;
				if (optionValue === "") {
					replaceDisabledException = exception;
					break;
				}
				exceptionsMap.set(optionValue, exception);
			}
			for (const filter of networkFilters) {
				const modifier = filter.getHtmlModifier();
				if (modifier === null) continue;
				const exception = replaceDisabledException || exceptionsMap.get(filter.optionValue);
				this.emit("filter-matched", {
					filter,
					exception
				}, {
					request,
					filterType: FilterType.NETWORK
				});
				if (exception === void 0) htmlSelectors.push(["replace", modifier]);
			}
		}
		if (htmlSelectors.length !== 0) this.emit("html-filtered", htmlSelectors, request.url);
		return htmlSelectors;
	}
	/**
	* Given `hostname` and `domain` of a page (or frame), return the list of
	* styles and scripts to inject in the page.
	*/
	getCosmeticsFilters({ url, hostname, domain, ancestors, classes, hrefs, ids, getBaseRules = true, getInjectionRules = true, getExtendedRules = true, getRulesFromDOM = true, getRulesFromHostname = true, hidingStyle, callerContext }) {
		if (this.config.loadCosmeticFilters === false) return {
			active: false,
			extended: [],
			scripts: [],
			styles: ""
		};
		const { matches, allowGenericHides } = this.matchCosmeticFilters({
			url,
			hostname,
			domain,
			ancestors,
			classes,
			hrefs,
			ids,
			getRulesFromDOM,
			getRulesFromHostname,
			getInjectionRules,
			getExtendedRules,
			callerContext
		});
		const filters = [];
		for (const { filter, exception } of matches) if (filter !== void 0 && exception === void 0) filters.push(filter);
		const { extended, scripts, styles } = this.injectCosmeticFilters(filters, {
			url,
			injectScriptlets: getInjectionRules,
			injectExtended: getExtendedRules,
			allowGenericHides,
			getBaseRules,
			hidingStyle
		});
		return {
			active: true,
			extended,
			scripts,
			styles
		};
	}
	/**
	* Prepares cosmetic filters to be injected by compiling them to stylesheets, scripts and extented selector ASTs.
	*/
	injectCosmeticFilters(filters, { url, injectStyles = true, injectScriptlets, injectExtended, allowGenericHides = true, getBaseRules, hidingStyle }) {
		const scripts = [];
		const styleFilters = [];
		const extendedFilters = [];
		for (const filter of filters) if (injectScriptlets && filter.isScriptInject()) {
			const script = filter.getScript(this.resources.getScriptlet.bind(this.resources));
			if (script !== void 0) scripts.push(script);
		} else if (filter.isExtended()) {
			if (injectExtended === true && this.config.loadExtendedSelectors) extendedFilters.push(filter);
		} else if (injectStyles === true) styleFilters.push(filter);
		const stylesheets = this.cosmetics.getStylesheetsFromFilters({
			filters: styleFilters,
			extendedFilters
		}, {
			getBaseRules,
			allowGenericHides,
			hidingStyle
		});
		for (const script of scripts) this.emit("script-injected", script, url);
		if (stylesheets.stylesheet.length !== 0) this.emit("style-injected", stylesheets.stylesheet, url);
		return {
			extended: stylesheets.extended,
			scripts,
			styles: stylesheets.stylesheet
		};
	}
	matchCosmeticFilters({ url, hostname, domain, ancestors, classes, hrefs, ids, getRulesFromDOM = true, getRulesFromHostname = true, getInjectionRules, getExtendedRules, callerContext }) {
		domain || (domain = "");
		const matches = [];
		const exceptions = this.hideExceptions.matchAll(Request.fromRawDetails({
			domain,
			hostname,
			url,
			sourceDomain: "",
			sourceHostname: "",
			sourceUrl: ""
		}), this.isFilterExcluded.bind(this));
		const genericHides = [];
		const specificHides = [];
		for (const filter of exceptions) {
			if (filter.isSpecificHide()) specificHides.push(filter);
			if (filter.isGenericHide()) genericHides.push(filter);
		}
		const genericHideException = findApplicableHideException(genericHides);
		const specificHideException = findApplicableHideException(specificHides);
		if (genericHideException !== void 0) {
			const match = {
				filter: void 0,
				exception: genericHideException
			};
			matches.push(match);
			this.emit("filter-matched", match, {
				url,
				callerContext,
				filterType: FilterType.COSMETIC
			});
		}
		if (specificHideException !== void 0 && genericHideException !== specificHideException) {
			const match = {
				filter: void 0,
				exception: specificHideException
			};
			matches.push(match);
			this.emit("filter-matched", match, {
				url,
				callerContext,
				filterType: FilterType.COSMETIC
			});
		}
		const { filters, unhides } = this.cosmetics.getCosmeticsFilters({
			domain,
			hostname,
			ancestors,
			classes,
			hrefs,
			ids,
			allowGenericHides: genericHideException === void 0,
			allowSpecificHides: specificHideException === void 0,
			getRulesFromDOM,
			getRulesFromHostname,
			isFilterExcluded: this.isFilterExcluded.bind(this)
		});
		let injectionsDisabledFilter = void 0;
		const unhideExceptions = /* @__PURE__ */ new Map();
		for (const unhide of unhides) if (unhide.isScriptInject() === true && unhide.isUnhide() === true && unhide.getSelector().length === 0) injectionsDisabledFilter = unhide;
		else unhideExceptions.set(normalizeSelector(unhide, this.resources.getScriptletCanonicalName.bind(this.resources)), unhide);
		for (const filter of filters) {
			if (filter.isExtended() && getExtendedRules === false) continue;
			let exception = unhideExceptions.get(normalizeSelector(filter, this.resources.getScriptletCanonicalName.bind(this.resources)));
			if (filter.isScriptInject()) {
				if (injectionsDisabledFilter !== void 0) exception = injectionsDisabledFilter;
				if (getInjectionRules === false) continue;
			}
			matches.push({
				filter,
				exception
			});
			this.emit("filter-matched", {
				filter,
				exception
			}, {
				url,
				callerContext,
				filterType: FilterType.COSMETIC
			});
		}
		return {
			matches,
			allowGenericHides: genericHideException === void 0
		};
	}
	/**
	* Given a `request`, return all matching network filters found in the engine.
	*/
	matchAll(request) {
		const filters = [];
		if (request.isSupported) {
			Array.prototype.push.apply(filters, this.importants.matchAll(request, this.isFilterExcluded.bind(this)));
			Array.prototype.push.apply(filters, this.filters.matchAll(request, this.isFilterExcluded.bind(this)));
			Array.prototype.push.apply(filters, this.exceptions.matchAll(request, this.isFilterExcluded.bind(this)));
			Array.prototype.push.apply(filters, this.csp.matchAll(request, this.isFilterExcluded.bind(this)));
			Array.prototype.push.apply(filters, this.hideExceptions.matchAll(request, this.isFilterExcluded.bind(this)));
			Array.prototype.push.apply(filters, this.redirects.matchAll(request, this.isFilterExcluded.bind(this)));
		}
		return new Set(filters);
	}
	/**
	* Given a "main_frame" request, check if some content security policies
	* should be injected in the page.
	*/
	getCSPDirectives(request) {
		if (!this.config.loadNetworkFilters) return;
		if (request.isSupported !== true || request.isMainFrame() === false) return;
		const matches = this.csp.matchAll(request, this.isFilterExcluded.bind(this));
		if (matches.length === 0) return;
		const cspExceptions = /* @__PURE__ */ new Map();
		const cspFilters = [];
		for (const filter of matches) if (filter.isException()) {
			if (filter.csp === void 0) {
				this.emit("filter-matched", { exception: filter }, {
					request,
					filterType: FilterType.NETWORK
				});
				return;
			}
			cspExceptions.set(filter.csp, filter);
		} else cspFilters.push(filter);
		if (cspFilters.length === 0) return;
		const enabledCsp = /* @__PURE__ */ new Set();
		for (const filter of cspFilters.values()) {
			const exception = cspExceptions.get(filter.csp);
			if (exception === void 0) enabledCsp.add(filter.csp);
			this.emit("filter-matched", {
				filter,
				exception
			}, {
				request,
				filterType: FilterType.NETWORK
			});
		}
		const csps = Array.from(enabledCsp).join("; ");
		if (csps.length > 0) this.emit("csp-injected", request, csps);
		return csps;
	}
	/**
	* Decide if a network request (usually from WebRequest API) should be
	* blocked, redirected or allowed.
	*/
	match(request, withMetadata = false) {
		const result = {
			exception: void 0,
			filter: void 0,
			match: false,
			redirect: void 0,
			rewrite: void 0,
			metadata: void 0
		};
		if (!this.config.loadNetworkFilters) return result;
		if (request.isSupported) {
			result.filter = this.importants.match(request, this.isFilterExcluded.bind(this));
			let redirectNone;
			let redirectRule;
			if (result.filter === void 0) {
				const redirects = this.redirects.matchAll(request, this.isFilterExcluded.bind(this)).sort((a, b) => b.getRedirectPriority() - a.getRedirectPriority());
				if (redirects.length !== 0) {
					for (const filter of redirects) if (filter.getRedirectResource() === "none") redirectNone = filter;
					else if (filter.isRedirectRule()) {
						if (redirectRule === void 0) redirectRule = filter;
					} else if (result.filter === void 0) result.filter = filter;
				}
				if (result.filter === void 0) {
					result.filter = this.filters.match(request, this.isFilterExcluded.bind(this));
					if (redirectRule !== void 0 && result.filter !== void 0) result.filter = redirectRule;
				}
				if (result.filter !== void 0) result.exception = this.exceptions.match(request, this.isFilterExcluded.bind(this));
				if (result.filter === void 0) {
					const searchParamSeparatorIndex = request.url.indexOf("?");
					if (searchParamSeparatorIndex !== -1 && searchParamSeparatorIndex !== request.url.length - 1) {
						const searchParamLiteral = request.url.slice(searchParamSeparatorIndex);
						const searchParams = new SearchParams(searchParamLiteral);
						let modified = false;
						const removeparamFilters = /* @__PURE__ */ new Map();
						const removeparamExceptions = /* @__PURE__ */ new Map();
						for (const filter of this.removeparams.matchAll(request, this.isFilterExcluded.bind(this))) if (filter.isException()) removeparamExceptions.set(filter.removeparam, filter);
						else removeparamFilters.set(filter.removeparam, filter);
						const removeparamIgnoreFilter = (result.filter === void 0 ? this.exceptions.match(request, this.isFilterExcluded.bind(this)) : result.exception) || removeparamExceptions.get("");
						for (const [key, filter] of removeparamFilters) {
							if (key === "") {
								if (removeparamIgnoreFilter === void 0) {
									for (const key of Array.from(searchParams.keys())) searchParams.delete(key);
									modified = true;
								}
								this.emit("filter-matched", {
									filter,
									exception: removeparamIgnoreFilter
								}, {
									request,
									filterType: FilterType.NETWORK
								});
								break;
							}
							if (!key.startsWith("~") && searchParamLiteral.slice(1, key.length + 1) !== key && !searchParamLiteral.includes(`&${key}`)) continue;
							const exception = removeparamExceptions.get(key) ?? removeparamIgnoreFilter;
							if (exception === void 0) if (key.startsWith("~")) {
								const inversionKey = key.slice(1);
								for (const param of Array.from(searchParams.keys())) if (param !== inversionKey && !removeparamExceptions.has(param)) {
									searchParams.delete(param);
									modified = true;
								}
							} else {
								searchParams.delete(key);
								modified = true;
							}
							this.emit("filter-matched", {
								filter,
								exception
							}, {
								request,
								filterType: FilterType.NETWORK
							});
						}
						if (modified) {
							let url = request.url.slice(0, searchParamSeparatorIndex);
							if (searchParams.size > 0) url += "?" + searchParams.toString();
							result.rewrite = { url };
						}
					}
				}
			}
			if (result.filter !== void 0 && result.exception === void 0 && result.filter.isRedirect()) if (redirectNone !== void 0) result.exception = redirectNone;
			else result.redirect = this.resources.getResource(result.filter.getRedirectResource());
		}
		result.match = result.exception === void 0 && result.filter !== void 0;
		if (result.filter) this.emit("filter-matched", {
			filter: result.filter,
			exception: result.exception
		}, {
			request,
			filterType: FilterType.NETWORK
		});
		if (result.exception !== void 0) this.emit("request-whitelisted", request, result);
		else if (result.redirect !== void 0) this.emit("request-redirected", request, result);
		else if (result.filter !== void 0) this.emit("request-blocked", request, result);
		else this.emit("request-allowed", request, result);
		if (withMetadata === true && result.filter !== void 0 && this.metadata) result.metadata = this.metadata.fromFilter(result.filter);
		return result;
	}
	getPatternMetadata(request, { getDomainMetadata = false } = {}) {
		if (this.metadata === void 0) return [];
		const seenPatterns = /* @__PURE__ */ new Set();
		const patterns = [];
		for (const filter of this.matchAll(request)) for (const patternInfo of this.metadata.fromFilter(filter)) if (!seenPatterns.has(patternInfo.pattern.key)) {
			seenPatterns.add(patternInfo.pattern.key);
			patterns.push(patternInfo);
		}
		if (getDomainMetadata) {
			for (const patternInfo of this.metadata.fromDomain(request.hostname)) if (!seenPatterns.has(patternInfo.pattern.key)) {
				seenPatterns.add(patternInfo.pattern.key);
				patterns.push(patternInfo);
			}
		}
		return patterns;
	}
	blockScripts() {
		this.updateFromDiff({ added: [block().scripts().redirectTo("javascript").toString()] });
		return this;
	}
	blockImages() {
		this.updateFromDiff({ added: [block().images().redirectTo("png").toString()] });
		return this;
	}
	blockMedias() {
		this.updateFromDiff({ added: [block().medias().redirectTo("mp4").toString()] });
		return this;
	}
	blockFrames() {
		this.updateFromDiff({ added: [block().frames().redirectTo("html").toString()] });
		return this;
	}
	blockFonts() {
		this.updateFromDiff({ added: [block().fonts().toString()] });
		return this;
	}
	blockStyles() {
		this.updateFromDiff({ added: [block().styles().toString()] });
		return this;
	}
};
//#endregion
export { FilterEngine as default };
