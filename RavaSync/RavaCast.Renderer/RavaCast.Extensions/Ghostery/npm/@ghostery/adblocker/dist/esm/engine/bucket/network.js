import NetworkFilter from "../../filters/network.js";
import { noopOptimizeNetwork, optimizeNetwork } from "../optimizer.js";
import ReverseIndex from "../reverse-index.js";
import FiltersContainer from "./filters.js";
//#region node_modules/@ghostery/adblocker/dist/esm/engine/bucket/network.js
/*!
* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/
/**
* Accelerating data structure for network filters matching.
*/
var NetworkFilterBucket = class NetworkFilterBucket {
	static deserialize(buffer, config) {
		const bucket = new NetworkFilterBucket({ config });
		bucket.index = ReverseIndex.deserialize(buffer, NetworkFilter.deserialize, config.enableOptimizations ? optimizeNetwork : noopOptimizeNetwork, config);
		bucket.badFilters = FiltersContainer.deserialize(buffer, NetworkFilter.deserialize, config);
		return bucket;
	}
	constructor({ filters = [], config }) {
		this.index = new ReverseIndex({
			config,
			deserialize: NetworkFilter.deserialize,
			filters: [],
			optimize: config.enableOptimizations ? optimizeNetwork : noopOptimizeNetwork
		});
		this.badFiltersIds = null;
		this.badFilters = new FiltersContainer({
			config,
			deserialize: NetworkFilter.deserialize,
			filters: []
		});
		if (filters.length !== 0) this.update(filters, void 0);
	}
	getFilters() {
		return [].concat(this.badFilters.getFilters(), this.index.getFilters());
	}
	update(newFilters, removedFilters) {
		const badFilters = [];
		const remaining = [];
		for (const filter of newFilters) if (filter.isBadFilter()) badFilters.push(filter);
		else remaining.push(filter);
		this.badFilters.update(badFilters, removedFilters);
		this.index.update(remaining, removedFilters);
		this.badFiltersIds = null;
	}
	getSerializedSize() {
		return this.badFilters.getSerializedSize() + this.index.getSerializedSize();
	}
	serialize(buffer) {
		this.index.serialize(buffer);
		this.badFilters.serialize(buffer);
	}
	matchAll(request, isFilterExcluded) {
		const filters = [];
		this.index.iterMatchingFilters(request.getTokens(), (filter) => {
			if (filter.match(request) && this.isFilterDisabled(filter) === false && !isFilterExcluded?.(filter)) filters.push(filter);
			return true;
		});
		return filters;
	}
	match(request, isFilterExcluded) {
		let match;
		this.index.iterMatchingFilters(request.getTokens(), (filter) => {
			if (filter.match(request) && this.isFilterDisabled(filter) === false && !isFilterExcluded?.(filter)) {
				match = filter;
				return false;
			}
			return true;
		});
		return match;
	}
	/**
	* Given a matching filter, check if it is disabled by a $badfilter.
	*/
	isFilterDisabled(filter) {
		if (this.badFiltersIds === null) {
			const badFilters = this.badFilters.getFilters();
			if (badFilters.length === 0) return false;
			const badFiltersIds = /* @__PURE__ */ new Set();
			for (const badFilter of badFilters) badFiltersIds.add(badFilter.getIdWithoutBadFilter());
			this.badFiltersIds = badFiltersIds;
		}
		return this.badFiltersIds.has(filter.getId());
	}
};
//#endregion
export { NetworkFilterBucket as default };
