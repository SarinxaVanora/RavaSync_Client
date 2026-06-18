import crc32 from "../../crc32.js";
import { EMPTY_UINT32_ARRAY, EMPTY_UINT8_ARRAY, StaticDataView } from "../../data-view.js";
//#region node_modules/@ghostery/adblocker/dist/esm/engine/bucket/filters.js
/*!
* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/
/**
* Generic filters container (for both CosmeticFilter and NetworkFilter
* instances). This abstracts away some of the logic to serialize/lazy-load
* lists of filters (which is useful for things like generic cosmetic filters
* or $badfilter).
*/
var FiltersContainer = class FiltersContainer {
	static merge(sources, opts) {
		if (sources.length < 2) throw new Error("FiltersContainer.merge requires at least two source containers.");
		const firstSource = sources[0];
		let numberOfFilters = 0;
		for (const source of sources) {
			if (source.config.debug === true) throw new Error("FiltersContainer.merge requires debug=false for every source.");
			if (source.config.enableCompression !== firstSource.config.enableCompression) throw new Error("FiltersContainer.merge requires matching compression settings.");
			numberOfFilters += source.numberOfFilters;
		}
		if (numberOfFilters === 0) return new FiltersContainer({
			config: firstSource.config,
			deserialize: firstSource.deserialize,
			filters: []
		});
		const hashFunc = typeof opts?.hashFunc === "function" ? opts.hashFunc : crc32;
		const filtersByHash = /* @__PURE__ */ new Map();
		for (const source of sources) {
			if (source.numberOfFilters === 0) continue;
			for (let i = 0, filterIndex, filterIndexEnd; i < source.numberOfFilters; i += 1) {
				filterIndex = source.offsets[i];
				filterIndexEnd = source.offsets[i + 1];
				filtersByHash.set(hashFunc(source.filters, filterIndex, filterIndexEnd), source.filters.subarray(filterIndex, filterIndexEnd));
			}
		}
		let filtersIndexSize = 0;
		for (const filter of filtersByHash.values()) filtersIndexSize += filter.byteLength;
		const view = StaticDataView.allocate(filtersIndexSize, firstSource.config);
		const offsets = new Uint32Array(filtersByHash.size + 1);
		let index = 0;
		for (const filter of filtersByHash.values()) {
			offsets[index++] = view.pos;
			view.buffer.set(filter, view.pos);
			view.setPos(view.pos + filter.byteLength);
		}
		offsets[index] = view.getPos();
		const container = new FiltersContainer({
			config: firstSource.config,
			deserialize: firstSource.deserialize,
			filters: []
		});
		container.filters = view.subarray();
		container.offsets = offsets;
		container.numberOfFilters = filtersByHash.size;
		return container;
	}
	static deserialize(buffer, deserialize, config) {
		const container = new FiltersContainer({
			deserialize,
			config,
			filters: []
		});
		const numberOfFilters = buffer.getUint32();
		container.numberOfFilters = numberOfFilters;
		if (numberOfFilters !== 0) {
			container.offsets = new Uint32Array(numberOfFilters + 1);
			for (let i = 0; i < container.offsets.length; i += 1) container.offsets[i] = buffer.getUint32();
			const filtersIndexSize = container.offsets[numberOfFilters];
			container.filters = buffer.buffer.subarray(buffer.pos, buffer.pos + filtersIndexSize);
			buffer.setPos(buffer.pos + filtersIndexSize);
		}
		return container;
	}
	constructor({ config, deserialize, filters }) {
		this.deserialize = deserialize;
		this.filters = EMPTY_UINT8_ARRAY;
		this.offsets = EMPTY_UINT32_ARRAY;
		this.numberOfFilters = 0;
		this.config = config;
		if (filters.length !== 0) this.update(filters, void 0);
	}
	/**
	* Update filters based on `newFilters` and `removedFilters`.
	*/
	update(newFilters, removedFilters) {
		let selected = [];
		const compression = this.config.enableCompression;
		const currentFilters = this.getFilters();
		if (currentFilters.length !== 0) {
			if (removedFilters === void 0 || removedFilters.size === 0) selected = currentFilters;
			else for (const filter of currentFilters) if (removedFilters.has(filter.getId()) === false) selected.push(filter);
		}
		const storedFiltersRemoved = selected.length !== currentFilters.length;
		const numberOfExistingFilters = selected.length;
		for (const filter of newFilters) selected.push(filter);
		const storedFiltersAdded = selected.length > numberOfExistingFilters;
		if (selected.length === 0) {
			this.filters = EMPTY_UINT8_ARRAY;
			this.offsets = EMPTY_UINT32_ARRAY;
			this.numberOfFilters = 0;
		} else if (storedFiltersAdded === true || storedFiltersRemoved === true) {
			if (this.config.debug === true) selected.sort((f1, f2) => f1.getId() - f2.getId());
			let bufferSizeEstimation = 0;
			for (const filter of selected) bufferSizeEstimation += filter.getSerializedSize(compression);
			const buffer = StaticDataView.allocate(bufferSizeEstimation, this.config);
			const offsets = new Uint32Array(selected.length + 1);
			for (let i = 0; i < selected.length; i += 1) {
				offsets[i] = buffer.getPos();
				selected[i].serialize(buffer);
			}
			offsets[selected.length] = buffer.getPos();
			this.filters = buffer.subarray();
			this.offsets = offsets;
			this.numberOfFilters = selected.length;
		}
	}
	getSerializedSize() {
		return 4 + this.offsets.byteLength + this.filters.byteLength;
	}
	serialize(buffer) {
		buffer.pushUint32(this.numberOfFilters);
		for (const offset of this.offsets) buffer.pushUint32(offset);
		buffer.buffer.set(this.filters, buffer.pos);
		buffer.setPos(buffer.pos + this.filters.byteLength);
	}
	getFilters() {
		if (this.numberOfFilters === 0) return [];
		const filters = [];
		const buffer = StaticDataView.fromUint8Array(this.filters, this.config);
		for (let i = 0; i < this.numberOfFilters; i += 1) filters.push(this.deserialize(buffer));
		return filters;
	}
};
//#endregion
export { FiltersContainer as default };
