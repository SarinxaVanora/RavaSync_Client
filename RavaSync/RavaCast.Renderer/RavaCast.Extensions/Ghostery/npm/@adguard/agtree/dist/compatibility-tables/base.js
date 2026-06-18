import { AdblockSyntax } from "../utils/adblockers.js";
import { isUndefined } from "../utils/type-guards.js";
import { GenericPlatform } from "./platforms.js";
import { isGenericPlatform } from "./utils/platform-helpers.js";
//#region node_modules/@adguard/agtree/dist/compatibility-tables/base.js
/**
* @file Provides common compatibility table methods.
*/
/**
* Base compatibility table class which provides common methods to work with compatibility data.
*
* @template T Compatibility data schema.
*/
var CompatibilityTableBase = class {
	/**
	* Compatibility table data.
	*/
	data;
	/**
	* Optional name transformer function. If provided,
	* it will be called in all methods before processing compatibility data names.
	*/
	nameTransformer;
	/**
	* Creates a new instance of the common compatibility table.
	*
	* @param data Compatibility table data.
	* @param nameTransformer Optional name transformer function.
	*/
	constructor(data, nameTransformer = null) {
		this.data = data;
		this.nameTransformer = nameTransformer;
	}
	/**
	* Helper method to get a 'row' from the compatibility table data by name.
	*
	* @param name Compatibility data name.
	* @returns Compatibility table row storage or `null` if not found.
	*/
	getRowStorage(name) {
		const idx = this.data.map[name];
		if (isUndefined(idx)) return null;
		return this.data.shared[idx];
	}
	/**
	* Checks whether a compatibility data `name` exists for any platform.
	*
	* @note Technically, do the same as `exists()` method with generic platform _any_
	* but it is faster because it does not apply complex logic.
	*
	* @param name Compatibility data name.
	*
	* @returns True if the compatibility data exists, false otherwise.
	*/
	existsAny(name) {
		const normalizedName = this.nameTransformer ? this.nameTransformer(name) : name;
		return !isUndefined(this.data.map[normalizedName]);
	}
	/**
	* Checks whether a compatibility data `name` exists for a specified platform.
	*
	* @param name Compatibility data name.
	* @param platform Specific or generic platform.
	*
	* @returns True if the compatibility data exists, false otherwise.
	*/
	exists(name, platform) {
		const normalizedName = this.nameTransformer ? this.nameTransformer(name) : name;
		const data = this.getRowStorage(normalizedName);
		if (!data) return false;
		const isMatch = (idx) => {
			const el = data.shared[idx];
			return !isUndefined(el) && (el.name === normalizedName || !!el.aliases?.includes(normalizedName));
		};
		if (isGenericPlatform(platform)) {
			const keys = Object.keys(data.map);
			for (let i = 0; i < keys.length; i += 1) {
				const key = Number(keys[i]);
				if (platform & key) {
					const idx = data.map[key];
					if (isMatch(idx)) return true;
				}
			}
			return false;
		}
		const idx = data.map[platform];
		return isMatch(idx);
	}
	/**
	* Returns a compatibility data by name and specific platform.
	*
	* @param name The name of the compatibility data.
	* @param platform The specific platform.
	*
	* @returns A single compatibility data or `null` if not found.
	*/
	getSingle(name, platform) {
		const normalizedName = this.nameTransformer ? this.nameTransformer(name) : name;
		const data = this.getRowStorage(normalizedName);
		if (!data) return null;
		const idx = data.map[platform];
		return isUndefined(idx) ? null : data.shared[idx];
	}
	/**
	* Returns all compatibility data records for name and specified platform.
	*
	* @param name Compatibility data name.
	* @param platform Specific or generic platform.
	*
	* @returns Multiple records grouped by platforms.
	* Technically, it is an object where keys are platform enums values and values are compatibility data records.
	*
	* @note Platform enum values can be converted to string names using {@link getSpecificPlatformName} on demand.
	*/
	getMultiple(name, platform) {
		const normalizedName = this.nameTransformer ? this.nameTransformer(name) : name;
		const data = this.getRowStorage(normalizedName);
		if (!data) return null;
		if (isGenericPlatform(platform)) {
			const result = {};
			const keys = Object.keys(data.map);
			for (let i = 0; i < keys.length; i += 1) {
				const key = Number(keys[i]);
				if (platform & key) {
					const idx = data.map[key];
					if (!isUndefined(idx)) result[key] = data.shared[idx];
				}
			}
			return result;
		}
		const idx = data.map[platform];
		if (isUndefined(idx)) return null;
		return { key: data.shared[idx] };
	}
	/**
	* Returns all compatibility data records for the specified platform.
	*
	* @param platform Specific or generic platform.
	*
	* @returns Array of multiple records grouped by platforms.
	*/
	getAllMultiple(platform) {
		const result = [];
		for (let i = 0; i < this.data.shared.length; i += 1) {
			const data = this.data.shared[i];
			new Set(data.shared.map(({ name }) => name)).forEach((name) => {
				const multipleRecords = this.getMultiple(name, platform);
				if (multipleRecords) result.push(multipleRecords);
			});
		}
		return result;
	}
	/**
	* Returns the first compatibility data record for name and specified platform.
	*
	* @param name Compatibility data name.
	* @param platform Specific, generic, or combined platform.
	*
	* @returns First found compatibility data record or `null` if not found.
	*/
	getFirst(name, platform) {
		const normalizedName = this.nameTransformer ? this.nameTransformer(name) : name;
		const data = this.getRowStorage(normalizedName);
		if (!data) return null;
		if (isGenericPlatform(platform)) {
			const keys = Object.keys(data.map);
			for (let i = 0; i < keys.length; i += 1) {
				const key = Number(keys[i]);
				if (platform & key) {
					const idx = data.map[key];
					if (!isUndefined(idx)) return data.shared[idx];
				}
			}
			return null;
		}
		const idx = data.map[platform];
		if (isUndefined(idx)) return null;
		return data.shared[idx];
	}
	/**
	* Returns all compatibility data records for the specified name.
	*
	* @param name Compatibility data name.
	*
	* @returns Array of multiple records grouped by platforms.
	*/
	getRow(name) {
		const normalizedName = this.nameTransformer ? this.nameTransformer(name) : name;
		const data = this.getRowStorage(normalizedName);
		if (!data) return [];
		return data.shared;
	}
	/**
	* Returns all compatibility data grouped by products.
	*
	* @returns Array of multiple records grouped by products.
	*/
	getRowsByProduct() {
		const result = [];
		for (let i = 0; i < this.data.shared.length; i += 1) {
			const data = this.data.shared[i];
			const keys = Object.keys(data.map);
			const row = {
				[AdblockSyntax.Adg]: {},
				[AdblockSyntax.Ubo]: {},
				[AdblockSyntax.Abp]: {}
			};
			for (let j = 0; j < keys.length; j += 1) {
				const key = Number(keys[j]);
				if (key & GenericPlatform.AdgAny) row[AdblockSyntax.Adg][key] = data.shared[data.map[key]];
				else if (key & GenericPlatform.UboAny) row[AdblockSyntax.Ubo][key] = data.shared[data.map[key]];
				else if (key & GenericPlatform.AbpAny) row[AdblockSyntax.Abp][key] = data.shared[data.map[key]];
			}
			result.push(row);
		}
		return result;
	}
};
//#endregion
export { CompatibilityTableBase };
