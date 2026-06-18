import logger_default from "./logger.js";
import { requireParam, requireString } from "./utils.js";
import { BadJobError } from "./errors.js";
import { random32Bit } from "./random.js";
import { isLegalVoteCount } from "./popularity-estimator.js";
import { sanitizeHostname, sanitizePathSegment } from "./popularity-vote-sanitizer.js";
//#region node_modules/@whotracksme/reporting/reporting/src/popularity-vote-handler.js
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
function throwBadJobError(text, job) {
	throw new BadJobError(`${text}\n--- begin job ---\n${JSON.stringify(job, null, 2)}\n--- end job ---`);
}
/**
* Processes the results from the PopularityEstimator. These two classes are
* tightly coupled but with different responsibilities:
* 1) The PopularityEstimator does all the bookkeeping (all locally)
*    - it counts visited websites
*    - in fixed intervals, it picks samples to vote on (e.g. visited a domain)
* 2) The PopularityVoteHandler prepares the votes (includes network calls):
*    - masking fields if necessary (using static checks, followed by a quorum request)
*    - once a vote is ready, it triggers the actual message sending
*/
var PopularityVoteHandler = class {
	constructor({ jobScheduler, communication, quorumChecker, countryProvider, pauseState }) {
		this.jobScheduler = requireParam(jobScheduler);
		this.communication = requireParam(communication);
		this.quorumChecker = requireParam(quorumChecker);
		this.countryProvider = requireParam(countryProvider);
		this.pauseState = pauseState;
		jobScheduler.registerHandler("popularity-estimator:prepare-voting:v1", async (job) => {
			const { urlProjection, countType, intervalType, sample } = job.args;
			const validate = (actual, allowed) => {
				if (!allowed.includes(actual)) throwBadJobError(`Job failed check ('${actual}' not in ${`[${allowed.join(", ")}]`})`, job);
			};
			validate(urlProjection, [
				"domain",
				"hostname",
				"hostnamePath"
			]);
			validate(countType, ["visits"]);
			validate(intervalType, [
				"1d",
				"1w",
				"4w"
			]);
			const unmaskedVote = sample?.value;
			const numVotes = sample?.count;
			if (typeof unmaskedVote !== "string" || !Number.isInteger(numVotes)) throwBadJobError("Corrupted vote sample in job", job);
			if (!isLegalVoteCount({
				numVotes,
				intervalType
			})) throwBadJobError(`Found too many votes in job (numVotes=${numVotes})`, job);
			const vote = await this.prepareVote(urlProjection, countType, intervalType, unmaskedVote);
			logger_default.info("Vote ready:", `${numVotes}x for`, vote);
			const sendVoteJob = {
				type: "popularity-estimator:send-vote:v1",
				args: { vote },
				config: { readyIn: {
					min: 2 * SECOND,
					max: 20 * MINUTE
				} }
			};
			return Array(numVotes).fill(sendVoteJob);
		}, {
			priority: -100,
			maxJobsTotal: 100,
			cooldownInMs: 4 * SECOND
		});
		jobScheduler.registerHandler("popularity-estimator:send-vote:v1", async (job) => {
			const { vote } = job.args;
			if (!vote) throwBadJobError("Vote missing in job", job);
			await this.communication.send({
				action: "wtm.popularity",
				payload: vote,
				ver: 3,
				"anti-duplicates": random32Bit()
			});
			logger_default.info("Voted successfully for:", vote);
		}, {
			priority: 100,
			maxJobsTotal: 100,
			cooldownInMs: 8 * SECOND
		});
		this._sanitizeHostname = sanitizeHostname;
		this._sanitizePathSegment = sanitizePathSegment;
	}
	async prepareVote(urlProjection, countType, intervalType, unmaskedVote) {
		const { hostname: unsafeHostname, path: unsafePath } = this._ensureThatVoteIsValid(urlProjection, unmaskedVote);
		let vote = [sanitizeHostname(unsafeHostname), ...unsafePath.map(sanitizePathSegment)].join("/");
		let adblocker;
		if (this.pauseState) adblocker = {
			paused: this.pauseState.isHostnamePaused(unsafeHostname),
			mode: this.pauseState.getFilteringMode()
		};
		await this.quorumChecker.sendQuorumIncrement({ text: vote });
		if (!await this.quorumChecker.checkQuorumConsent({ text: vote })) {
			logger_default.info("Vote failed quorum. Discard value:", vote);
			vote = "--";
		}
		const payload = {
			type: {
				urlProjection,
				countType,
				intervalType
			},
			vote,
			ctry: this.countryProvider.getSafeCountryCode()
		};
		if (adblocker) payload.adblocker = adblocker;
		return payload;
	}
	_ensureThatVoteIsValid(urlProjection, unmaskedVote) {
		requireString(unmaskedVote);
		switch (urlProjection) {
			case "domain":
			case "hostname": return {
				hostname: this._validateHostname(unmaskedVote),
				path: []
			};
			case "hostnamePath": {
				const [hostname, ...path] = unmaskedVote.split("/");
				return {
					hostname: this._validateHostname(hostname),
					path
				};
			}
			default: throw new BadJobError(`Unexpected urlProjection type: ${urlProjection}`);
		}
	}
	_validateHostname(hostname) {
		if (!/^([a-zA-Z0-9-]+\.)*[a-zA-Z0-9-]+$/.test(hostname)) throw new BadJobError(`Invalid hostname: ${hostname}`);
		return hostname;
	}
};
//#endregion
export { PopularityVoteHandler as default };
