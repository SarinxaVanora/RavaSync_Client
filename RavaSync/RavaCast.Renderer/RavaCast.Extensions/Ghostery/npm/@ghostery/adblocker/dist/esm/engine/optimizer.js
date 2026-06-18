import { setBit } from "../utils.js";
import { Domains } from "./domains.js";
import NetworkFilter, { NETWORK_FILTER_MASK } from "../filters/network.js";
//#region node_modules/@ghostery/adblocker/dist/esm/engine/optimizer.js
/*!
* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/
function processRegex(r) {
	return `(?:${r.source})`;
}
function escape(s) {
	return `(?:${s.replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&")})`;
}
function setWithDefault(map, key, value) {
	let bucket = map.get(key);
	if (bucket === void 0) {
		bucket = [];
		map.set(key, bucket);
	}
	bucket.push(value);
}
function groupBy(filters, criteria) {
	const grouped = /* @__PURE__ */ new Map();
	for (const filter of filters) setWithDefault(grouped, criteria(filter), filter);
	return Array.from(grouped.values());
}
function splitBy(filters, condition) {
	const positive = [];
	const negative = [];
	for (const filter of filters) if (condition(filter)) positive.push(filter);
	else negative.push(filter);
	return {
		negative,
		positive
	};
}
var OPTIMIZATIONS = [
	{
		description: "Remove duplicated filters by ID",
		fusion: (filters) => filters[0],
		groupByCriteria: (filter) => "" + filter.getId(),
		select: () => true
	},
	{
		description: "Group idential filter with same mask but different domains in single filters",
		fusion: (filters) => {
			const parts = [];
			const hostnames = /* @__PURE__ */ new Set();
			const notHostnames = /* @__PURE__ */ new Set();
			const entities = /* @__PURE__ */ new Set();
			const notEntities = /* @__PURE__ */ new Set();
			for (const { domains } of filters) if (domains !== void 0) {
				if (domains.parts !== void 0) parts.push(domains.parts);
				if (domains.hostnames !== void 0) for (const hash of domains.hostnames) hostnames.add(hash);
				if (domains.entities !== void 0) for (const hash of domains.entities) entities.add(hash);
				if (domains.notHostnames !== void 0) for (const hash of domains.notHostnames) notHostnames.add(hash);
				if (domains.notEntities !== void 0) for (const hash of domains.notEntities) notEntities.add(hash);
			}
			return new NetworkFilter(Object.assign({}, filters[0], {
				domains: new Domains({
					hostnames: hostnames.size !== 0 ? new Uint32Array(hostnames).sort() : void 0,
					entities: entities.size !== 0 ? new Uint32Array(entities).sort() : void 0,
					notHostnames: notHostnames.size !== 0 ? new Uint32Array(notHostnames).sort() : void 0,
					notEntities: notEntities.size !== 0 ? new Uint32Array(notEntities).sort() : void 0,
					parts: parts.length !== 0 ? parts.join(",") : void 0
				}),
				rawLine: filters[0].rawLine !== void 0 ? filters.map(({ rawLine }) => rawLine).join(" <+> ") : void 0
			}));
		},
		groupByCriteria: (filter) => filter.getHostname() + filter.getFilter() + filter.getMask() + (filter.optionValue ?? ""),
		select: (filter) => !filter.isCSP() && filter.denyallow === void 0 && filter.domains !== void 0
	},
	{
		description: "Group simple patterns, into a single filter",
		fusion: (filters) => {
			const patterns = [];
			for (const f of filters) if (f.isRegex()) patterns.push(processRegex(f.getRegex()));
			else if (f.isRightAnchor()) patterns.push(`${escape(f.getFilter())}$`);
			else if (f.isLeftAnchor()) patterns.push(`^${escape(f.getFilter())}`);
			else patterns.push(escape(f.getFilter()));
			return new NetworkFilter(Object.assign({}, filters[0], {
				mask: setBit(filters[0].mask, NETWORK_FILTER_MASK.isRegex),
				rawLine: filters[0].rawLine !== void 0 ? filters.map(({ rawLine }) => rawLine).join(" <+> ") : void 0,
				regex: new RegExp(patterns.join("|"), "i")
			}));
		},
		groupByCriteria: (filter) => "" + (filter.getMask() & ~NETWORK_FILTER_MASK.isRegex & ~NETWORK_FILTER_MASK.isFullRegex),
		select: (filter) => filter.domains === void 0 && filter.denyallow === void 0 && !filter.isHostnameAnchor() && !filter.isRedirect() && !filter.isCSP() && !filter.isCaseSensitive
	}
];
/**
* Optimizer which returns the list of original filters.
*/
function noopOptimizeNetwork(filters) {
	return filters;
}
function noopOptimizeCosmetic(filters) {
	return filters;
}
/**
* Fusion a set of `filters` by applying optimizations sequentially.
*/
function optimizeNetwork(filters) {
	const fused = [];
	let toFuse = filters;
	for (const { select, fusion, groupByCriteria } of OPTIMIZATIONS) {
		const { positive, negative } = splitBy(toFuse, select);
		toFuse = negative;
		const groups = groupBy(positive, groupByCriteria);
		for (const group of groups) if (group.length > 1) fused.push(fusion(group));
		else toFuse.push(group[0]);
	}
	for (const filter of toFuse) fused.push(filter);
	return fused;
}
//#endregion
export { noopOptimizeCosmetic, noopOptimizeNetwork, optimizeNetwork };
