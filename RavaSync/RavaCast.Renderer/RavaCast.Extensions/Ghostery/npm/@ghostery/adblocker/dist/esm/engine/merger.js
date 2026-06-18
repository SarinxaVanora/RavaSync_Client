import Config from "../config.js";
import Preprocessor from "../preprocessor.js";
import Resources from "../resources.js";
import { noopOptimizeCosmetic, noopOptimizeNetwork, optimizeNetwork } from "./optimizer.js";
import ReverseIndex from "./reverse-index.js";
import FiltersContainer from "./bucket/filters.js";
import CosmeticFilterBucket from "./bucket/cosmetic.js";
import NetworkFilterBucket from "./bucket/network.js";
import HTMLBucket from "./bucket/html.js";
import { Metadata } from "./metadata.js";
import PreprocessorBucket from "./bucket/preprocessor.js";
//#region node_modules/@ghostery/adblocker/dist/esm/engine/merger.js
/*!
* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/
function mergeMetadata(engines) {
	const metadata = {
		organizations: {},
		categories: {},
		patterns: {}
	};
	for (const engine of engines) if (engine.metadata !== void 0) {
		for (const organization of engine.metadata.organizations.getValues()) if (metadata.organizations[organization.key] === void 0) metadata.organizations[organization.key] = organization;
		for (const category of engine.metadata.categories.getValues()) if (metadata.categories[category.key] === void 0) metadata.categories[category.key] = category;
		for (const pattern of engine.metadata.patterns.getValues()) if (metadata.patterns[pattern.key] === void 0) metadata.patterns[pattern.key] = pattern;
	}
	return metadata;
}
function mergeLists(engines) {
	const lists = /* @__PURE__ */ new Map();
	for (const engine of engines) for (const [key, value] of engine.lists) {
		if (lists.has(key)) continue;
		lists.set(key, value);
	}
	return lists;
}
function mergePreprocessors(engines) {
	const preprocessors = [];
	for (const engine of engines) for (const preprocessor of engine.preprocessors.preprocessors) {
		const local = preprocessors.find((local) => local.condition === preprocessor.condition);
		if (local === void 0) {
			preprocessors.push(new Preprocessor({
				condition: preprocessor.condition,
				filterIDs: new Set(preprocessor.filterIDs)
			}));
			continue;
		}
		for (const filterID of preprocessor.filterIDs) local.filterIDs.add(filterID);
	}
	return preprocessors;
}
function hasMetadata(metadata) {
	return Object.keys(metadata.categories).length + Object.keys(metadata.organizations).length + Object.keys(metadata.patterns).length !== 0;
}
/**
* Legacy semantic merge implementation, moved out of `FilterEngine.merge` so it
* can live next to byte-level merging during the transition.
*/
function legacyMerge(self, engines, { skipResources = false, overrideConfig = {} } = {}) {
	if (!engines || engines.length < 2) throw new Error("merging engines requires at least two engines");
	for (const engine of engines) if (engine.config.enableCompression !== engines[0].config.enableCompression) throw new Error(`compression of all merged engines must match with the first one: "${engines[0].config.enableCompression}" but got: "${engine.config.enableCompression}"`);
	const networkFilters = /* @__PURE__ */ new Map();
	const cosmeticFilters = /* @__PURE__ */ new Map();
	const metadata = mergeMetadata(engines);
	const lists = mergeLists(engines);
	const preprocessors = mergePreprocessors(engines);
	for (const engine of engines) {
		const filters = engine.getFilters();
		for (const networkFilter of filters.networkFilters) networkFilters.set(networkFilter.getId(), networkFilter);
		for (const cosmeticFilter of filters.cosmeticFilters) cosmeticFilters.set(cosmeticFilter.getId(), cosmeticFilter);
	}
	const engine = new self({
		networkFilters: Array.from(networkFilters.values()),
		cosmeticFilters: Array.from(cosmeticFilters.values()),
		preprocessors,
		lists,
		config: new Config({
			...engines[0].config,
			...overrideConfig
		})
	});
	if (hasMetadata(metadata)) engine.metadata = new Metadata(metadata);
	if (skipResources !== true) {
		for (const engine of engines.slice(1)) if (engine.resources.checksum !== engines[0].resources.checksum) throw new Error(`resource checksum of all merged engines must match with the first one: "${engines[0].resources.checksum}" but got: "${engine.resources.checksum}"`);
		engine.resources = Resources.copy(engines[0].resources);
	}
	return engine;
}
function mergeNetworkFilterBucket(sources, config, hashFunc) {
	const bucket = new NetworkFilterBucket({ config });
	const optimize = config.enableOptimizations ? optimizeNetwork : noopOptimizeNetwork;
	bucket.index = ReverseIndex.merge(sources.map((source) => source.index), config, optimize, { hashFunc });
	bucket.badFilters = FiltersContainer.merge(sources.map((source) => source.badFilters), { hashFunc });
	return bucket;
}
function mergeCosmeticFilterBucket(sources, config, hashFunc) {
	const bucket = new CosmeticFilterBucket({ config });
	bucket.genericRules = FiltersContainer.merge(sources.map((source) => source.genericRules), { hashFunc });
	bucket.classesIndex = ReverseIndex.merge(sources.map((source) => source.classesIndex), config, noopOptimizeCosmetic, { hashFunc });
	bucket.hostnameIndex = ReverseIndex.merge(sources.map((source) => source.hostnameIndex), config, noopOptimizeCosmetic, { hashFunc });
	bucket.hrefsIndex = ReverseIndex.merge(sources.map((source) => source.hrefsIndex), config, noopOptimizeCosmetic, { hashFunc });
	bucket.idsIndex = ReverseIndex.merge(sources.map((source) => source.idsIndex), config, noopOptimizeCosmetic, { hashFunc });
	bucket.unhideIndex = ReverseIndex.merge(sources.map((source) => source.unhideIndex), config, noopOptimizeCosmetic, { hashFunc });
	return bucket;
}
function mergeHTMLBucket(sources, config, hashFunc) {
	const bucket = new HTMLBucket({ config });
	const optimize = config.enableOptimizations ? optimizeNetwork : noopOptimizeNetwork;
	if (config.loadNetworkFilters === true) {
		bucket.networkIndex = ReverseIndex.merge(sources.map((source) => source.networkIndex), config, optimize, { hashFunc });
		bucket.exceptionsIndex = ReverseIndex.merge(sources.map((source) => source.exceptionsIndex), config, optimize, { hashFunc });
	}
	if (config.loadCosmeticFilters === true) {
		bucket.cosmeticIndex = ReverseIndex.merge(sources.map((source) => source.cosmeticIndex), config, noopOptimizeCosmetic, { hashFunc });
		bucket.unhideIndex = ReverseIndex.merge(sources.map((source) => source.unhideIndex), config, noopOptimizeCosmetic, { hashFunc });
	}
	return bucket;
}
function binaryMerge(self, engines, { skipResources = false, overrideConfig = {}, hashFunc } = {}) {
	if (!engines || engines.length < 2) throw new Error("merging engines requires at least two engines");
	for (const engine of engines) {
		if (engine.config.enableCompression !== engines[0].config.enableCompression) throw new Error(`compression of all merged engines must match with the first one: "${engines[0].config.enableCompression}" but got: "${engine.config.enableCompression}"`);
		if (engine.config.debug === true) throw new Error("merging engines with binaryMerge method is not allowed with debug mode strictly!");
	}
	if (overrideConfig.debug === true) throw new Error(`the resulting engine cannot have debug or compression when merging engines with binaryMerge method!`);
	if (typeof overrideConfig.enableCompression === "boolean" && overrideConfig.enableCompression !== engines[0].config.enableCompression) throw new Error(`the resulting engine should have same compression config when merging engines!`);
	const metadata = mergeMetadata(engines);
	const lists = mergeLists(engines);
	const config = new Config({
		...engines[0].config,
		...overrideConfig
	});
	const engine = new self({
		config,
		lists
	});
	engine.preprocessors = new PreprocessorBucket({ preprocessors: config.loadPreprocessors === true ? mergePreprocessors(engines) : [] });
	if (config.loadNetworkFilters === true) {
		engine.importants = mergeNetworkFilterBucket(engines.map((source) => source.importants), config, hashFunc);
		engine.redirects = mergeNetworkFilterBucket(engines.map((source) => source.redirects), config, hashFunc);
		engine.removeparams = mergeNetworkFilterBucket(engines.map((source) => source.removeparams), config, hashFunc);
		engine.filters = mergeNetworkFilterBucket(engines.map((source) => source.filters), config, hashFunc);
		engine.exceptions = config.loadExceptionFilters === true ? mergeNetworkFilterBucket(engines.map((source) => source.exceptions), config, hashFunc) : new NetworkFilterBucket({ config });
		engine.csp = config.loadCSPFilters === true ? mergeNetworkFilterBucket(engines.map((source) => source.csp), config, hashFunc) : new NetworkFilterBucket({ config });
		engine.hideExceptions = mergeNetworkFilterBucket(engines.map((source) => source.hideExceptions), config, hashFunc);
	}
	if (config.loadCosmeticFilters === true) engine.cosmetics = mergeCosmeticFilterBucket(engines.map((source) => source.cosmetics), config, hashFunc);
	if (config.enableHtmlFiltering === true) engine.htmlFilters = mergeHTMLBucket(engines.map((source) => source.htmlFilters), config, hashFunc);
	if (hasMetadata(metadata)) engine.metadata = new Metadata(metadata);
	if (skipResources !== true) {
		for (const engine of engines.slice(1)) if (engine.resources.checksum !== engines[0].resources.checksum) throw new Error(`resource checksum of all merged engines must match with the first one: "${engines[0].resources.checksum}" but got: "${engine.resources.checksum}"`);
		engine.resources = Resources.copy(engines[0].resources);
	}
	return engine;
}
//#endregion
export { binaryMerge, legacyMerge };
