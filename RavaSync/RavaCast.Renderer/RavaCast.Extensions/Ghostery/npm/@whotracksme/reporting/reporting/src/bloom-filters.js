import logger_default from "./logger.js";
import { fastHash } from "./utils.js";
import SelfCheck from "./self-check.js";
import PersistedBitarray from "./persisted-bitarray.js";
//#region node_modules/@whotracksme/reporting/reporting/src/bloom-filters.js
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
var MONTH = 30 * (24 * (60 * (60 * 1e3)));
function gcd(a, b) {
	return b === 0 ? a : gcd(b, a % b);
}
function allPairwiseRelativePrime(values) {
	for (let i = 0; i < values.length; i += 1) for (let j = i + 1; j < values.length; j += 1) if (gcd(values[i], values[j]) !== 1) return false;
	return true;
}
/**
* A bloom filter that is persisted.
*
* The implementation is bases on a "One-Hashing Bloom Filter"
* (see https://yangtonghome.github.io/uploads/One_Hashing.pdf).
* It uses only one hashing function, but splits the array in multiple partitions.
* The length of the partitions are expected to be pairwise relatively prime.
* That means, their gcd must be 1. For instance, you can take different prime numbers
* (https://gist.github.com/philipp-classen/318c870eb848b84548448eb4b80eed55).
*
* In OHBF, the number of partitions is comparable to "k" (the number of
* hashing functions) in a standard bloom filter and similar trade-offs apply.
*/
var BloomFilter = class {
	constructor({ database, name, prefix = "", version = 1, partitions, maxGenerations = 2, rotationIntervalInMs = 3 * MONTH }) {
		if (partitions.length === 0 || partitions.some((x) => x <= 0)) throw new Error("Partitions must be all non-empty and greater than zero");
		this.partitions = partitions;
		this.totalSize = partitions.reduce((x, y) => x + y);
		this.db = database;
		if (!name) throw new Error("Expected a name");
		if (name.includes("|")) throw new Error(`name=${name} it must not include the separator character |`);
		this.name = name;
		this.version = `v${version}`;
		if (this.version.includes("|")) throw new Error(`version=${this.version} it must not include the separator character |`);
		this.keyPrefix = `${prefix}bf|${name}|`;
		this.keyPrefixWithVersion = `${this.keyPrefix}${this.version}|`;
		if (maxGenerations <= 0) throw new Error(`At least one generation is needed (got: ${maxGenerations})`);
		if (rotationIntervalInMs < 0) throw new Error(`Rotation interval must not be negative (got: ${rotationIntervalInMs})`);
		if (maxGenerations > 1 && rotationIntervalInMs === 0) throw new Error("Rotations can only be disabled if you also set one generation");
		this.maxGenerations = maxGenerations;
		this.rotationIntervalInMs = rotationIntervalInMs;
		const logPrefix = `[bf=<${name}>]`;
		this._debug = logger_default.debug.bind(logger_default.debug, logPrefix);
		this._info = logger_default.info.bind(logger_default.info, logPrefix);
		this._warn = logger_default.warn.bind(logger_default.warn, logPrefix);
		this._error = logger_default.error.bind(logger_default.error, logPrefix);
	}
	_isOurKey(key) {
		return key.startsWith(this.keyPrefix);
	}
	_isKeyFromCurrentVersion(key) {
		return key.startsWith(this.keyPrefixWithVersion);
	}
	async ready(now = Date.now()) {
		if (!this._ready) this._ready = (async () => {
			const allKeys = await this.db.keys();
			const keysByGen = /* @__PURE__ */ new Map();
			const obsoleteKeys = [];
			const corruptedKeys = [];
			const unknownKeys = [];
			for (const key of allKeys) if (this._isKeyFromCurrentVersion(key)) {
				const [genString, shard] = key.slice(this.keyPrefixWithVersion.length).split("|");
				if (genString && shard) {
					const gen = Number.parseInt(genString, 10);
					if (gen > 0) {
						const entry = keysByGen.get(gen);
						if (entry) entry.push(key);
						else keysByGen.set(gen, [key]);
						continue;
					}
				}
				corruptedKeys.push(key);
			} else if (this._isOurKey(key)) obsoleteKeys.push(key);
			else unknownKeys.push(key);
			const pendingCleanups = [];
			if (corruptedKeys.length + obsoleteKeys.length > 0) {
				if (corruptedKeys.length > 0) this._warn(corruptedKeys.length, "corrupted keys found (will be deleted):", corruptedKeys);
				if (obsoleteKeys.length > 0) this._info(obsoleteKeys.length, "keys from old version found (will be deleted):", obsoleteKeys);
				pendingCleanups.push(async () => {
					try {
						await Promise.all([...corruptedKeys, ...obsoleteKeys].map((key) => this.db.delete(key)));
						this._info("Successfully cleaned up keys");
					} catch (e) {
						this._warn("Failed to cleanup keys", e);
					}
				});
			}
			if (unknownKeys.length > 0) {
				this._warn(unknownKeys.length, "unknown keys found (use a dedicated database per bloom filter)");
				this._debug("List of unknown keys:", unknownKeys);
			}
			const gensToDelete = [];
			const currentGens = [...keysByGen.keys()].sort((x, y) => x - y);
			if (currentGens.length === 0) this._debug("No existing generations found");
			const isTooNew = (ts) => ts > now + 2 * this.rotationIntervalInMs;
			while (currentGens.length > 0 && isTooNew(currentGens[currentGens.length - 1])) {
				const newestGen = currentGens.pop();
				gensToDelete.push(newestGen);
				this._warn("Clock jump detected: purging corrupted generation:", newestGen);
			}
			if (this.maxGenerations > 1) {
				const isTooOld = (ts) => ts < now - this.rotationIntervalInMs * this.maxGenerations;
				while (currentGens.size > 0 && isTooOld(currentGens[0])) {
					const oldestGen = currentGens.shift();
					gensToDelete.push(oldestGen);
					this._info("detected old generation:", oldestGen);
				}
			}
			if (currentGens.length === 0 || this.maxGenerations > 1 && currentGens[currentGens.length - 1] < now - this.rotationIntervalInMs) {
				this._info("Start new generation:", now);
				currentGens.push(now);
			}
			const rotatedOut = currentGens.splice(0, currentGens.length - this.maxGenerations);
			if (rotatedOut.length > 0) {
				this._info("Rotated out the following generations:", rotatedOut);
				gensToDelete.push(...rotatedOut);
			}
			for (const gen of gensToDelete) pendingCleanups.push(async () => {
				this._info("Deleting generation", gen);
				const keys = keysByGen.get(gen);
				try {
					await Promise.all(keys.map((key) => this.db.delete(key)));
					this._info(`Generation ${gen} successfully deleted (${keys.length} keys in total)`);
				} catch (e) {
					this._warn("Failed to delete keys from generation", gen, e);
				}
			});
			await Promise.all(pendingCleanups);
			this.generations = currentGens.map((gen) => new PersistedBitarray({
				database: this.db,
				size: this.totalSize,
				prefix: `${this.keyPrefixWithVersion}${gen}|`,
				name: `bf=<${this.name}:${gen}}>`
			}));
		})();
		await this._ready;
	}
	async add(value) {
		await this.ready();
		const latestGeneration = this.generations[this.generations.length - 1];
		const bitsToSet = this._computeBits(value);
		await latestGeneration.setMany(bitsToSet);
	}
	async mightContain(value, { updateTTLIfFound = false } = {}) {
		await this.ready();
		const bitsToTest = this._computeBits(value);
		for (let i = this.generations.length - 1; i >= 0; i -= 1) if (await this.generations[i].testMany(bitsToTest) && updateTTLIfFound && i !== this.generations.length - 1) {
			await this.generations[this.generations.length - 1].setMany(bitsToTest);
			return true;
		}
		return false;
	}
	_computeBits(value) {
		const hash = fastHash(value, { output: "number" });
		let offset = 0;
		return this.partitions.map((partitionSize) => {
			const posInPartition = hash % partitionSize;
			const index = offset + posInPartition;
			offset += partitionSize;
			return index;
		});
	}
	async selfChecks(check = new SelfCheck()) {
		if (!allPairwiseRelativePrime(this.partitions)) check.warn("partitions should be pairwise relative prime", { partitions: this.partitions });
		if (this.generations) await Promise.all(this.generations.map((gen) => gen.selfChecks(check.for(gen.name))));
		return check;
	}
};
//#endregion
export { BloomFilter };
