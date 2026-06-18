import { EMPTY_UINT32_ARRAY, StaticDataView, sizeOfBytes } from "../data-view.js";
import { nextPow2 } from "./reverse-index.js";
//#region node_modules/@ghostery/adblocker/dist/esm/engine/map.js
/*!
* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/
var EMPTY_BUCKET = Number.MAX_SAFE_INTEGER >>> 0;
/**
* This is a simpler version of reverse-index data structure which implements
* a simple Map-like class, backed by compact typed arrays. This means that
* the structure can be serialized to a typed array very quickly and loaded
* back instantly.
*/
var CompactMap = class CompactMap {
	static deserialize(buffer, deserialize) {
		const tokensLookupIndexSize = buffer.getUint32();
		const bucketsIndexSize = buffer.getUint32();
		const numberOfValues = buffer.getUint32();
		const view = StaticDataView.fromUint8Array(buffer.getBytes(true), { enableCompression: false });
		const tokensLookupIndex = view.getUint32ArrayView(tokensLookupIndexSize);
		const bucketsIndex = view.getUint32ArrayView(bucketsIndexSize);
		const valuesIndexStart = view.pos;
		view.seekZero();
		return new CompactMap({
			deserialize,
			values: [],
			getKeys: () => [],
			getSerializedSize: () => 0,
			serialize: () => {}
		}).updateInternals({
			bucketsIndex,
			valuesIndexStart,
			numberOfValues,
			tokensLookupIndex,
			view
		});
	}
	constructor({ serialize, deserialize, getKeys, getSerializedSize, values }) {
		this.cache = /* @__PURE__ */ new Map();
		this.bucketsIndex = EMPTY_UINT32_ARRAY;
		this.tokensLookupIndex = EMPTY_UINT32_ARRAY;
		this.valuesIndexStart = 0;
		this.numberOfValues = 0;
		this.view = StaticDataView.empty({ enableCompression: false });
		this.deserializeValue = deserialize;
		if (values.length !== 0) {
			const patternsKeys = [];
			let bucketsIndexSize = 0;
			let estimatedBufferSize = 0;
			for (const value of values) estimatedBufferSize += getSerializedSize(value);
			if (values.length === 0) {
				this.updateInternals({
					bucketsIndex: EMPTY_UINT32_ARRAY,
					valuesIndexStart: 0,
					numberOfValues: 0,
					tokensLookupIndex: EMPTY_UINT32_ARRAY,
					view: StaticDataView.empty({ enableCompression: false })
				});
				return;
			}
			for (const value of values) {
				const keys = getKeys(value);
				patternsKeys.push(keys);
				bucketsIndexSize += 2 * keys.length;
			}
			estimatedBufferSize += bucketsIndexSize * 4;
			const tokensLookupIndexSize = Math.max(2, nextPow2(values.length));
			const mask = tokensLookupIndexSize - 1;
			const suffixes = [];
			for (let i = 0; i < tokensLookupIndexSize; i += 1) suffixes.push([]);
			estimatedBufferSize += tokensLookupIndexSize * 4;
			const buffer = StaticDataView.allocate(estimatedBufferSize, { enableCompression: false });
			const tokensLookupIndex = buffer.getUint32ArrayView(tokensLookupIndexSize);
			const bucketsIndex = buffer.getUint32ArrayView(bucketsIndexSize);
			const valuesIndexStart = buffer.getPos();
			for (let i = 0; i < patternsKeys.length; i += 1) {
				const value = values[i];
				const keys = patternsKeys[i];
				const valueIndex = buffer.pos;
				serialize(value, buffer);
				for (const key of keys) suffixes[key & mask].push([key, valueIndex]);
			}
			let indexInBucketsIndex = 0;
			for (let i = 0; i < tokensLookupIndexSize; i += 1) {
				const valuesForMask = suffixes[i];
				tokensLookupIndex[i] = indexInBucketsIndex;
				for (const [token, valueIndex] of valuesForMask) {
					bucketsIndex[indexInBucketsIndex++] = token;
					bucketsIndex[indexInBucketsIndex++] = valueIndex;
				}
			}
			this.updateInternals({
				bucketsIndex,
				valuesIndexStart,
				numberOfValues: patternsKeys.length,
				tokensLookupIndex,
				view: buffer
			});
		}
	}
	updateInternals({ bucketsIndex, valuesIndexStart, numberOfValues, tokensLookupIndex, view }) {
		this.bucketsIndex = bucketsIndex;
		this.valuesIndexStart = valuesIndexStart;
		this.numberOfValues = numberOfValues;
		this.tokensLookupIndex = tokensLookupIndex;
		this.view = view;
		view.seekZero();
		return this;
	}
	getValues() {
		const values = [];
		if (this.numberOfValues === 0) return values;
		this.view.setPos(this.valuesIndexStart);
		for (let i = 0; i < this.numberOfValues; i += 1) values.push(this.deserializeValue(this.view));
		this.view.seekZero();
		return values;
	}
	/**
	* Estimate the number of bytes needed to serialize this instance of `Map`.
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
		buffer.pushUint32(this.numberOfValues);
		buffer.pushBytes(this.view.buffer, true);
	}
	get(key) {
		const cachedValues = this.cache.get(key);
		if (cachedValues !== void 0) return cachedValues;
		const offset = key & this.tokensLookupIndex.length - 1;
		const startOfBucket = this.tokensLookupIndex[offset];
		if (startOfBucket === EMPTY_BUCKET) return [];
		const endOfBucket = offset === this.tokensLookupIndex.length - 1 ? this.bucketsIndex.length : this.tokensLookupIndex[offset + 1];
		const valuesIndices = [];
		for (let i = startOfBucket; i < endOfBucket; i += 2) if (this.bucketsIndex[i] === key) valuesIndices.push(this.bucketsIndex[i + 1]);
		if (valuesIndices.length === 0) return [];
		const values = [];
		const view = this.view;
		for (let i = 0; i < valuesIndices.length; i += 1) {
			view.setPos(valuesIndices[i]);
			values.push(this.deserializeValue(view));
		}
		this.cache.set(key, values);
		return values;
	}
};
//#endregion
export { CompactMap };
