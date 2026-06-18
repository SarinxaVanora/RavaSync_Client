import crc32 from "../crc32.js";
import { EMPTY_UINT32_ARRAY, StaticDataView, sizeOfBytes } from "../data-view.js";
//#region node_modules/@ghostery/adblocker/dist/esm/engine/reverse-index.js
/*!
* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/
function nextPow2(v) {
	v--;
	v |= v >> 1;
	v |= v >> 2;
	v |= v >> 4;
	v |= v >> 8;
	v |= v >> 16;
	v++;
	return v;
}
/**
* Generate unique IDs for requests, which is used to avoid matching the same
* buckets multiple times on the same request (which can happen if a token
* appears more than once in a URL).
*/
var UID = 1;
function getNextId() {
	const id = UID;
	UID = (UID + 1) % 1e9;
	return id;
}
var EMPTY_BUCKET = Number.MAX_SAFE_INTEGER >>> 0;
/**
* The ReverseIndex is an accelerating data structure which allows finding a
* subset of the filters given a list of tokens seen in a URL. It is the core
* of the adblocker's matching capabilities and speed.
*
* It has mainly two caracteristics:
* 1. It is very compact and is able to load fast.
* 2. It is *very fast* in finding potential candidates.
*
* Conceptually, the reverse index dispatches filters in "buckets" (an array of
* one or more filters). Filters living in the same bucket are guaranteed to
* share at least one of their tokens (appearing in the pattern). For example:
*
*   - Bucket 1 (ads):
*       - /ads.js
*       - /script/ads/tracking.js
*       - /ads/
*   - Bucket 2 (tracking)
*       - /tracking.js
*       - ||tracking.com/cdn
*
* We see that filters in "Bucket 1" are indexed using the token "ads" and
* "Bucket 2" using token "tracking".
*
* This property allows to quickly discard most of the filters when we match a
* URL. To achieve this, the URL is tokenized in the same way filters are
* tokenized and for each token, we check if there are some filters available.
*
* For example:
*
*  URL "https://tracking.com/" has the following tokens: "https", "tracking"
*  and "com". We immediatly see that we only check the two filters in the
*  "tracking" bucket since they are the only ones having a common token with
*  the URL.
*
* How do we pick the token for each filter?
* =========================================
*
* Each filter is only indexed *once*, which means that we need to pick one of
* the tokens appearing in the pattern. We choose the token such that each
* filter is indexed using the token which was the *least seen* globally. In
* other words, we pick the most discriminative token for each filter. This is
* done using the following algorithm:
*   1. Tokenize all the filters which will be stored in the index
*   2. Compute a histogram of frequency of each token (globally)
*   3. Select the best token for each filter (lowest frequency)
*/
var ReverseIndex = class ReverseIndex {
	static merge(sources, config, optimize, opts) {
		if (sources.length < 2) throw new Error("ReverseIndex.merge requires at least two source indexes.");
		const firstSource = sources[0];
		for (const source of sources) {
			if (source.config.debug === true) throw new Error("ReverseIndex.merge requires debug=false for every source.");
			if (source.config.enableCompression !== firstSource.config.enableCompression) throw new Error("ReverseIndex.merge requires matching compression settings.");
		}
		if (config.debug === true) throw new Error("ReverseIndex.merge requires debug=false for target config.");
		if (config.enableCompression !== firstSource.config.enableCompression) throw new Error("ReverseIndex.merge requires target config compression to match sources.");
		if (sources.reduce((total, source) => total + source.numberOfFilters, 0) === 0) return new ReverseIndex({
			config,
			deserialize: firstSource.deserializeFilter,
			filters: [],
			optimize
		});
		const hashFunc = typeof opts?.hashFunc === "function" ? opts.hashFunc : crc32;
		const filtersByHash = /* @__PURE__ */ new Map();
		for (let sourceIndex = 0; sourceIndex < sources.length; sourceIndex += 1) {
			const source = sources[sourceIndex];
			const tokensByOffset = /* @__PURE__ */ new Map();
			for (let i = 1; i < source.bucketsIndex.length; i += 2) {
				const arr = tokensByOffset.get(source.bucketsIndex[i]);
				if (typeof arr === "undefined") tokensByOffset.set(source.bucketsIndex[i], [source.bucketsIndex[i - 1]]);
				else arr.push(source.bucketsIndex[i - 1]);
			}
			if (tokensByOffset.size !== source.numberOfFilters) throw new Error(`ReverseIndex.merge cannot infer filter byte ranges for source ${sourceIndex}: ${source.numberOfFilters} filters were serialized, but only ${tokensByOffset.size} have bucket entries.`);
			const aligned = Array.from(tokensByOffset.keys()).sort(function(a, b) {
				return a - b;
			});
			aligned.push(source.view.buffer.byteLength);
			for (let i = 1, hash, tokens, filter; i < aligned.length; i++) {
				hash = hashFunc(source.view.buffer, aligned[i - 1], aligned[i]);
				tokens = tokensByOffset.get(aligned[i - 1]);
				filter = filtersByHash.get(hash);
				if (typeof filter === "undefined" || filter.tokens.length < tokens.length) filtersByHash.set(hash, {
					tokens: tokensByOffset.get(aligned[i - 1]),
					bytes: source.view.buffer.subarray(aligned[i - 1], aligned[i])
				});
			}
		}
		let totalNumberOfIndexedTokens = 0;
		let filtersIndexSize = 0;
		for (const filter of filtersByHash.values()) {
			totalNumberOfIndexedTokens += filter.tokens.length;
			filtersIndexSize += filter.bytes.byteLength;
		}
		const bucketsIndexSize = totalNumberOfIndexedTokens * 2;
		const tokensLookupIndexSize = Math.max(2, nextPow2(totalNumberOfIndexedTokens));
		const view = StaticDataView.allocate(tokensLookupIndexSize * 4 + bucketsIndexSize * 4 + filtersIndexSize, config);
		const tokensLookupIndex = view.getUint32ArrayView(tokensLookupIndexSize);
		const bucketsIndex = view.getUint32ArrayView(bucketsIndexSize);
		const filtersIndexStart = view.getPos();
		const mask = tokensLookupIndexSize - 1;
		const suffixes = [];
		for (let i = 0; i < tokensLookupIndexSize; i += 1) suffixes.push([]);
		for (const filter of filtersByHash.values()) {
			const filterIndex = view.getPos();
			view.buffer.set(filter.bytes, filterIndex);
			view.setPos(filterIndex + filter.bytes.byteLength);
			for (const token of filter.tokens) suffixes[token & mask].push([token, filterIndex]);
		}
		let indexInBucketsIndex = 0;
		for (let i = 0; i < tokensLookupIndexSize; i += 1) {
			const filtersForMask = suffixes[i];
			tokensLookupIndex[i] = indexInBucketsIndex;
			for (const [token, filterIndex] of filtersForMask) {
				bucketsIndex[indexInBucketsIndex++] = token;
				bucketsIndex[indexInBucketsIndex++] = filterIndex;
			}
		}
		view.seekZero();
		return new ReverseIndex({
			config,
			deserialize: firstSource.deserializeFilter,
			filters: [],
			optimize
		}).updateInternals({
			bucketsIndex,
			filtersIndexStart,
			numberOfFilters: filtersByHash.size,
			tokensLookupIndex,
			view
		});
	}
	static deserialize(buffer, deserialize, optimize, config) {
		const tokensLookupIndexSize = buffer.getUint32();
		const bucketsIndexSize = buffer.getUint32();
		const numberOfFilters = buffer.getUint32();
		const view = StaticDataView.fromUint8Array(buffer.getBytes(true), config);
		const tokensLookupIndex = view.getUint32ArrayView(tokensLookupIndexSize);
		const bucketsIndex = view.getUint32ArrayView(bucketsIndexSize);
		const filtersIndexStart = view.pos;
		view.seekZero();
		return new ReverseIndex({
			config,
			deserialize,
			filters: [],
			optimize
		}).updateInternals({
			bucketsIndex,
			filtersIndexStart,
			numberOfFilters,
			tokensLookupIndex,
			view
		});
	}
	constructor({ deserialize, filters, optimize, config }) {
		this.bucketsIndex = EMPTY_UINT32_ARRAY;
		this.filtersIndexStart = 0;
		this.numberOfFilters = 0;
		this.tokensLookupIndex = EMPTY_UINT32_ARRAY;
		this.cache = /* @__PURE__ */ new Map();
		this.view = StaticDataView.empty(config);
		this.deserializeFilter = deserialize;
		this.optimize = optimize;
		this.config = config;
		if (filters.length !== 0) this.update(filters, void 0);
	}
	/**
	* Load all filters from this index in memory (i.e.: deserialize them from
	* the byte array into NetworkFilter or CosmeticFilter instances). This is
	* mostly useful for debugging or testing purposes.
	*/
	getFilters() {
		const filters = [];
		if (this.numberOfFilters === 0) return filters;
		this.view.setPos(this.filtersIndexStart);
		for (let i = 0; i < this.numberOfFilters; i += 1) filters.push(this.deserializeFilter(this.view));
		this.view.seekZero();
		return filters;
	}
	/**
	* Return an array of all the tokens currently used as keys of the "buckets index".
	*/
	getTokens() {
		const tokens = /* @__PURE__ */ new Set();
		for (let i = 0; i < this.bucketsIndex.length; i += 2) tokens.add(this.bucketsIndex[i]);
		return new Uint32Array(tokens);
	}
	/**
	* Estimate the number of bytes needed to serialize this instance of `ReverseIndex`.
	*/
	getSerializedSize() {
		return 12 + sizeOfBytes(this.view.buffer, true);
	}
	/**
	* Dump this index to `buffer`.
	*/
	serialize(buffer) {
		buffer.pushUint32(this.tokensLookupIndex.length);
		buffer.pushUint32(this.bucketsIndex.length);
		buffer.pushUint32(this.numberOfFilters);
		buffer.pushBytes(this.view.buffer, true);
	}
	/**
	* Iterate on all filters found in buckets associated with the given list of
	* tokens. The callback is called on each of them. Early termination can be
	* achieved if the callback returns `false`.
	*
	* This will not check if each filter returned would match a given request but
	* is instead used as a list of potential candidates (much smaller than the
	* total set of filters; typically between 5 and 10 filters will be checked).
	*/
	iterMatchingFilters(tokens, cb) {
		const requestId = getNextId();
		for (const token of tokens) if (this.iterBucket(token, requestId, cb) === false) return;
		this.iterBucket(0, requestId, cb);
	}
	/**
	* Re-create the internal data-structure of the reverse index *in-place*. It
	* needs to be called with a list of new filters and optionally a list of ids
	* (as returned by either NetworkFilter.getId() or CosmeticFilter.getId())
	* which need to be removed from the index.
	*/
	update(newFilters, removedFilters) {
		if (this.cache.size !== 0) this.cache.clear();
		const compression = this.config.enableCompression;
		let totalNumberOfTokens = 0;
		let totalNumberOfIndexedFilters = 0;
		const filtersTokens = [];
		let bucketsIndexSize = 0;
		let estimatedBufferSize = this.view.buffer.byteLength - this.filtersIndexStart;
		let filters = this.getFilters();
		if (filters.length !== 0) {
			if (removedFilters !== void 0 && removedFilters.size !== 0) filters = filters.filter((f) => {
				if (removedFilters.has(f.getId())) {
					estimatedBufferSize -= f.getSerializedSize(compression);
					return false;
				}
				return true;
			});
			for (const filter of newFilters) {
				estimatedBufferSize += filter.getSerializedSize(compression);
				filters.push(filter);
			}
		} else {
			filters = newFilters;
			for (const filter of newFilters) estimatedBufferSize += filter.getSerializedSize(compression);
		}
		if (filters.length === 0) {
			this.updateInternals({
				bucketsIndex: EMPTY_UINT32_ARRAY,
				filtersIndexStart: 0,
				numberOfFilters: 0,
				tokensLookupIndex: EMPTY_UINT32_ARRAY,
				view: StaticDataView.empty(this.config)
			});
			return;
		}
		if (this.config.debug === true) filters.sort((f1, f2) => f1.getId() - f2.getId());
		const histogram = new Uint32Array(Math.max(nextPow2(2 * filters.length), 256));
		for (const filter of filters) {
			const multiTokens = filter.getTokens();
			filtersTokens.push(multiTokens);
			bucketsIndexSize += 2 * multiTokens.length;
			totalNumberOfIndexedFilters += multiTokens.length;
			for (const tokens of multiTokens) {
				totalNumberOfTokens += tokens.length;
				for (const token of tokens) histogram[token % histogram.length] += 1;
			}
		}
		estimatedBufferSize += bucketsIndexSize * 4;
		const tokensLookupIndexSize = Math.max(2, nextPow2(totalNumberOfIndexedFilters));
		const mask = tokensLookupIndexSize - 1;
		const suffixes = [];
		for (let i = 0; i < tokensLookupIndexSize; i += 1) suffixes.push([]);
		estimatedBufferSize += tokensLookupIndexSize * 4;
		const buffer = StaticDataView.allocate(estimatedBufferSize, this.config);
		const tokensLookupIndex = buffer.getUint32ArrayView(tokensLookupIndexSize);
		const bucketsIndex = buffer.getUint32ArrayView(bucketsIndexSize);
		const filtersIndexStart = buffer.getPos();
		for (let i = 0; i < filtersTokens.length; i += 1) {
			const filter = filters[i];
			const multiTokens = filtersTokens[i];
			const filterIndex = buffer.pos;
			filter.serialize(buffer);
			for (const tokens of multiTokens) {
				let bestToken = 0;
				let minCount = totalNumberOfTokens + 1;
				for (const token of tokens) {
					const tokenCount = histogram[token % histogram.length];
					if (tokenCount < minCount) {
						minCount = tokenCount;
						bestToken = token;
						if (minCount === 1) break;
					}
				}
				suffixes[bestToken & mask].push([bestToken, filterIndex]);
			}
		}
		let indexInBucketsIndex = 0;
		for (let i = 0; i < tokensLookupIndexSize; i += 1) {
			const filtersForMask = suffixes[i];
			tokensLookupIndex[i] = indexInBucketsIndex;
			for (const [token, filterIndex] of filtersForMask) {
				bucketsIndex[indexInBucketsIndex++] = token;
				bucketsIndex[indexInBucketsIndex++] = filterIndex;
			}
		}
		buffer.seekZero();
		this.updateInternals({
			bucketsIndex,
			filtersIndexStart,
			numberOfFilters: filtersTokens.length,
			tokensLookupIndex,
			view: buffer
		});
	}
	updateInternals({ bucketsIndex, filtersIndexStart, numberOfFilters, tokensLookupIndex, view }) {
		this.bucketsIndex = bucketsIndex;
		this.filtersIndexStart = filtersIndexStart;
		this.numberOfFilters = numberOfFilters;
		this.tokensLookupIndex = tokensLookupIndex;
		this.view = view;
		view.seekZero();
		return this;
	}
	/**
	* If a bucket exists for the given token, call the callback on each filter
	* found inside. An early termination mechanism is built-in, to stop iterating
	* as soon as `false` is returned from the callback.
	*/
	iterBucket(token, requestId, cb) {
		let bucket = this.config.enableInMemoryCache === true ? this.cache.get(token) : void 0;
		if (bucket === void 0) {
			const offset = token & this.tokensLookupIndex.length - 1;
			const startOfBucket = this.tokensLookupIndex[offset];
			if (startOfBucket === EMPTY_BUCKET) return true;
			const endOfBucket = offset === this.tokensLookupIndex.length - 1 ? this.bucketsIndex.length : this.tokensLookupIndex[offset + 1];
			const filtersIndices = [];
			for (let i = startOfBucket; i < endOfBucket; i += 2) if (this.bucketsIndex[i] === token) filtersIndices.push(this.bucketsIndex[i + 1]);
			if (filtersIndices.length === 0) return true;
			const filters = [];
			const view = this.view;
			for (let i = 0; i < filtersIndices.length; i += 1) {
				view.setPos(filtersIndices[i]);
				filters.push(this.deserializeFilter(view));
			}
			bucket = {
				filters: filters.length > 1 ? this.optimize(filters) : filters,
				lastRequestSeen: -1
			};
			if (this.config.enableInMemoryCache === true) this.cache.set(token, bucket);
		}
		if (bucket.lastRequestSeen !== requestId) {
			bucket.lastRequestSeen = requestId;
			const filters = bucket.filters;
			for (let i = 0; i < filters.length; i += 1) if (cb(filters[i]) === false) {
				if (i > 0) {
					const filter = filters[i];
					filters[i] = filters[i - 1];
					filters[i - 1] = filter;
				}
				return false;
			}
		}
		return true;
	}
};
//#endregion
export { ReverseIndex as default, nextPow2 };
