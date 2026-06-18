import { parse } from "../../../../tldts-experimental/dist/es6/index.js";
import logger_default from "./logger.js";
import SeqExecutor from "./seq-executor.js";
import { requireInt, requireParam, requireString, requireUTC } from "./utils.js";
import SelfCheck from "./self-check.js";
import { randomSafeIntBetween } from "./random.js";
import PersistedCounters from "./persisted-counters.js";
//#region node_modules/@whotracksme/reporting/reporting/src/popularity-estimator.js
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
var SECOND = 1e3;
var MINUTE = 60 * SECOND;
var DAY = 24 * (60 * MINUTE);
var WEEK = 7 * DAY;
/**
* Increase this only if you want all state (the persisted counters
* and the rotation config) to be cleared.
*/
var DB_VERSION = 1;
function intervalTypeToMillseconds(intervalType) {
	switch (intervalType) {
		case "1d": return DAY;
		case "1w": return WEEK;
		case "4w": return 4 * WEEK;
		default: throw new Error(`Unexpected interval type: <<${intervalType}>>`);
	}
}
function chooseNumSamplesPerInterval(intervalType) {
	switch (intervalType) {
		case "1d": return 3;
		case "1w": return 3;
		case "4w": return 3;
		default: throw new Error(`Unexpected interval type: <<${intervalType}>>`);
	}
}
function isLegalVoteCount({ numVotes, intervalType }) {
	requireInt(numVotes);
	return numVotes >= 0 && numVotes <= chooseNumSamplesPerInterval(intervalType);
}
var PopularityEstimator = class {
	constructor({ storage, storageKey, connectDatabase, jobScheduler }) {
		this.active = false;
		this.storage = requireParam(storage);
		this.storageKey = requireString(storageKey);
		this.jobScheduler = requireParam(jobScheduler);
		this._popularityBy = {};
		this._allCounters = [];
		for (const urlProjection of [
			"domain",
			"hostname",
			"hostnamePath"
		]) {
			this._popularityBy[urlProjection] = { visits: {} };
			for (const interval of [
				"1d",
				"1w",
				"4w"
			]) {
				const name = `${urlProjection}::visits::${interval}`;
				const counter = new PersistedCounters({
					name,
					db: connectDatabase(`popularity_estimator::${name}`, { version: DB_VERSION })
				});
				this._popularityBy[urlProjection].visits[interval] = counter;
				this._allCounters.push(counter);
			}
		}
		this._visitCooldown = {
			hostname: "",
			expireAt: 0
		};
		this._nextRotationCheck = 0;
		this._criticalSection = new SeqExecutor();
		this._rotationConfig = null;
		this._rotationConfigPendingWrite = null;
		this._rotationFailuresInARow = 0;
	}
	_forEachCounter(cb) {
		for (const [urlProjection, level2] of Object.entries(this._popularityBy)) for (const [countType, level3] of Object.entries(level2)) for (const [intervalType, persistedCounter] of Object.entries(level3)) cb(urlProjection, countType, intervalType, persistedCounter);
	}
	async init() {
		this.active = true;
	}
	unload() {
		this.active = false;
		this.flush().catch(logger_default.error);
	}
	async flush() {
		await Promise.all([this._flushCounters(), this._criticalSection.run(async () => {}).then(() => this._flushCounters())]);
	}
	async _flushCounters() {
		await Promise.all(this._allCounters.map((counter) => counter.flush().catch((e) => {
			logger_default.error(`Failed to flush counter: ${counter}`, e);
		})));
	}
	onPageEvent(event) {
		if (!this.active) return;
		if (event.type === "safe-page-navigation") {
			const { hostname, pathname } = new URL(event.url);
			const { domain, isIp } = parse(hostname, {
				extractHostname: false,
				mixedInputs: false,
				validateHostname: false
			});
			if (isIp) {
				logger_default.info("[popularity] ignoring visit, because hostname is IP address:", event.url);
				return;
			}
			if (!domain) {
				logger_default.warn("Invalid domain detected in visited URL:", event.url);
				return;
			}
			const now = Date.now();
			try {
				if (event.isHistoryNavigation && now < this._visitCooldown.expireAt && this._visitCooldown.hostname === hostname) {
					logger_default.debug("[popularity] skip visit (cooldown not reached)", event.url);
					return;
				}
			} finally {
				this._visitCooldown = {
					hostname,
					expireAt: now + 20 * SECOND
				};
			}
			this._criticalSection.run(async () => {
				await this._runRotationChecks(now);
				if (this._rotationFailuresInARow !== 0) {
					logger_default.warn("Skipping visits, because the last rotation failed:", event);
					return;
				}
				const hostnamePath = hostname + pathname;
				for (const [interval, samplingSize] of [
					["1d", 1],
					["1w", 10],
					["4w", 100]
				]) if (randomSafeIntBetween(1, samplingSize) === 1) {
					this._popularityBy.domain.visits[interval].count(domain);
					this._popularityBy.hostname.visits[interval].count(hostname);
					this._popularityBy.hostnamePath.visits[interval].count(hostnamePath);
				}
			}).catch((e) => {
				logger_default.error("Unexpected error", e);
			});
		}
	}
	async _runRotationChecks(now) {
		if (requireUTC(now) >= this._nextRotationCheck) try {
			this._rotationConfig ||= await this._loadRotationConfig(now);
			await this._expireRotationCooldowns(now);
			await this._processCompletedRotations(now);
			this._scheduleNextRotationCheck();
			this._rotationFailuresInARow = 0;
		} catch (e) {
			logger_default.error("Error while rotating", e);
			this._rotationFailuresInARow += 1;
			this._nextRotationCheck = now + this._rotationFailuresInARow * SECOND;
			if (this._rotationFailuresInARow >= 3) try {
				logger_default.error("Too many rotation errors in a row. Purging state in an attempt to recover");
				await this.purgeAllState();
				logger_default.info("Successfully purged the state. Rotations will be retried on the next visit.");
				this._nextRotationCheck = now;
			} catch (e) {
				logger_default.error("Failed to purge the state. Perhaps the browser profile is broken?", e);
				this._nextRotationCheck = Number.MAX_SAFE_INTEGER;
			}
		}
	}
	async _loadRotationConfig(now) {
		requireUTC(now);
		let config = await this.storage.get(this.storageKey);
		if (!config) logger_default.info("No rotation config exists found yet.", "This should only happen on the first time the extension is started.");
		else if (config.dbVersion !== DB_VERSION) {
			logger_default.warn(`dbVersion mismatch: expected ${DB_VERSION}, but got ${config.dbVersion}. `, "Discarding it and replacing it by a fresh rotation configuration.");
			await this.purgeAllState().catch((e) => logger_default.warn("Failed to purge the state. Continuing...", e));
			config = null;
		} else if (config.lastUpdatedAt > now + 2 * DAY) {
			logger_default.error("Reverse clock jump detected: lastUpdatedAt", `(${new Date(config.lastUpdatedAt)})`, `is significantly ahead of the current time (${new Date(now)}).`, "Trying to reset the state and continuing...");
			await this.purgeAllState().catch((e) => logger_default.warn("Failed to purge the state. Continuing...", e));
			config = null;
		} else {
			logger_default.debug("Successfully loaded rotation config:", config);
			return config;
		}
		if (!config) {
			config = this._createInitialRotationConfig(now);
			this._rotationConfig = config;
			this._markRotationConfigDirty(now);
			logger_default.info("Created initial rotation config:", config);
		}
		return config;
	}
	_markRotationConfigDirty(now) {
		if (!this._rotationConfig) throw new Error("Illegal state: rotationConfig must have been loaded before being marked as dirty");
		this._rotationConfig.lastUpdatedAt = requireUTC(now);
		if (this._rotationConfigPendingWrite === null) this._rotationConfigPendingWrite = setTimeout(() => {
			this._rotationConfigPendingWrite = null;
			(async () => {
				try {
					await this.storage.set(this.storageKey, this._rotationConfig);
					logger_default.debug("Rotation config successfully persisted");
				} catch (e) {
					logger_default.error("Failed to persist rotation config", e);
				}
			})();
		}, 0);
	}
	_createInitialRotationConfig(now) {
		requireUTC(now);
		const cooldowns = {};
		this._forEachCounter((urlProjection, countType, intervalType) => {
			const expireAt = randomSafeIntBetween(now, now + intervalTypeToMillseconds(intervalType));
			cooldowns[urlProjection] ||= {};
			cooldowns[urlProjection][countType] ||= {};
			cooldowns[urlProjection][countType][intervalType] = expireAt;
			logger_default.info("Scheduled cooldown:", {
				urlProjection,
				countType,
				intervalType,
				endsAt: new Date(expireAt)
			});
		});
		return {
			dbVersion: DB_VERSION,
			lastUpdatedAt: now,
			cooldowns
		};
	}
	async _expireRotationCooldowns(now) {
		requireUTC(now);
		const { cooldowns } = this._rotationConfig;
		if (!cooldowns) return;
		let stillActive = 0;
		const pendingIO = [];
		this._forEachCounter((urlProjection, countType, intervalType, persistedCounter) => {
			const expireAt = cooldowns[urlProjection]?.[countType]?.[intervalType];
			if (expireAt !== void 0) if (now >= expireAt) {
				logger_default.info("Rotation cooldown expired. Ensuring that counters are empty:", {
					urlProjection,
					countType,
					intervalType
				});
				delete cooldowns[urlProjection][countType][intervalType];
				pendingIO.push(persistedCounter.clear());
				const endsAt = now + intervalTypeToMillseconds(intervalType);
				this._rotationConfig.rotations ||= {};
				this._rotationConfig.rotations[urlProjection] ||= {};
				this._rotationConfig.rotations[urlProjection][countType] ||= {};
				this._rotationConfig.rotations[urlProjection][countType][intervalType] = endsAt;
				this._markRotationConfigDirty(now);
				logger_default.info("Scheduled initial rotation:", {
					urlProjection,
					countType,
					intervalType,
					endsAt
				});
			} else stillActive += 1;
		});
		if (pendingIO.length > 0) {
			await Promise.all(pendingIO);
			logger_default.debug("Successfully cleared counter.");
		}
		if (stillActive === 0) {
			logger_default.info("All rotation cooldowns have expired.");
			delete this._rotationConfig.cooldowns;
			this._markRotationConfigDirty(now);
		}
	}
	async _processCompletedRotations(now) {
		requireUTC(now);
		const { rotations } = this._rotationConfig;
		if (!rotations) return;
		const pendingRotations = [];
		this._forEachCounter((urlProjection, countType, intervalType, persistedCounter) => {
			const expireAt = rotations[urlProjection]?.[countType]?.[intervalType];
			if (expireAt !== void 0 && now >= expireAt) {
				const elapsedSinceEndOfLastPeriod = now - expireAt;
				const defaultRotationPeriod = intervalTypeToMillseconds(intervalType);
				const periodsMissed = Math.floor(elapsedSinceEndOfLastPeriod / defaultRotationPeriod);
				const periodStart = expireAt + periodsMissed * defaultRotationPeriod;
				const newExpireAt = periodStart + randomSafeIntBetween(Math.round(.9 * defaultRotationPeriod), Math.round(1.1 * defaultRotationPeriod));
				rotations[urlProjection][countType][intervalType] = newExpireAt;
				this._markRotationConfigDirty(now);
				logger_default.info("Rotation period ended and a new one started:", {
					urlProjection,
					countType,
					intervalType,
					periodsMissed,
					newPeriod: {
						startsAt: periodStart,
						expiresAt: newExpireAt
					}
				});
				pendingRotations.push((async () => {
					const numSamples = chooseNumSamplesPerInterval(intervalType);
					const groupedSamples = await persistedCounter.sample(numSamples, { group: true });
					await persistedCounter.clear();
					return groupedSamples.map(([value, count]) => ({
						type: "popularity-estimator:prepare-voting:v1",
						args: {
							urlProjection,
							countType,
							intervalType,
							sample: {
								value,
								count
							}
						},
						config: {
							min: 0,
							max: 2 * MINUTE
						}
					}));
				})());
			}
		});
		if (pendingRotations.length > 0) {
			const allJobs = (await Promise.all(pendingRotations)).flat();
			try {
				await this.jobScheduler.registerJobs(allJobs);
			} catch (e) {
				logger_default.error("Failed to register jobs. These were lost:", allJobs);
			}
		}
	}
	async purgeAllState() {
		const errors = (await Promise.allSettled([this.storage.remove(this.storageKey), ...this._allCounters.map((counter) => counter.clear())])).filter((result) => result.status === "rejected");
		if (errors.length === 0) logger_default.info("Successfully cleared all state (including rotation config and counters)");
		else {
			errors.forEach(({ reason }) => logger_default.error("Error while clearing the state:", reason));
			throw new Error("Failed to clear state", { cause: errors[0].reason });
		}
	}
	_scheduleNextRotationCheck() {
		let nextCheckAt = Number.MAX_SAFE_INTEGER;
		const check = (expireAt) => {
			if (expireAt !== void 0 && expireAt < nextCheckAt) nextCheckAt = expireAt;
		};
		const { cooldowns = {}, rotations = {} } = this._rotationConfig;
		this._forEachCounter((urlProjection, countType, intervalType) => {
			check(cooldowns[urlProjection]?.[countType]?.[intervalType]);
			check(rotations[urlProjection]?.[countType]?.[intervalType]);
		});
		if (nextCheckAt === Number.MAX_SAFE_INTEGER) throw new Error("Illegal state: there must be at least one cooldown or rotation");
		logger_default.debug("Schedule next rotation at", nextCheckAt);
		this._nextRotationCheck = nextCheckAt;
	}
	async selfChecks(check = new SelfCheck()) {
		if (this._rotationConfig) {
			const { cooldowns, rotations } = this._rotationConfig;
			if (!cooldowns && !rotations) check.fail("Lack of cooldowns and rotations means the system cannot make progress");
		}
		if (this._rotationFailuresInARow > 0) {
			const failuresInARow = this._rotationFailuresInARow;
			check.warn("Last rotation failed. All visit will be currently discarded.", { failuresInARow });
		}
		if (this._rotationConfigPendingWrite !== null) check.warn("Rotation config modified but not synced to disk yet.");
		return check;
	}
	async runRotationChecks(now = Date.now()) {
		return this._criticalSection.run(async () => this._runRotationChecks(now));
	}
};
//#endregion
export { PopularityEstimator as default, isLegalVoteCount };
