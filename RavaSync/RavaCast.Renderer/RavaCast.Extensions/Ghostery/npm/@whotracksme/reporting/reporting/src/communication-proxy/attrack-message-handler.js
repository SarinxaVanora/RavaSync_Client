import logger_default from "../logger.js";
import { requireParam, requireString } from "../utils.js";
//#region node_modules/@whotracksme/reporting/reporting/src/communication-proxy/attrack-message-handler.js
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
var HOUR = 60 * MINUTE;
var DAY = 24 * HOUR;
var ATTRACK_JOB_TYPE = "attrack:send-message:v1";
var AttrackMessageHandler = class {
	constructor({ communication, jobScheduler }) {
		this.communication = requireParam(communication);
		this.jobScheduler = requireParam(jobScheduler);
		this._configOverride = {
			"wtm.attrack.tokensv2": { readyIn: {
				min: 7 * MINUTE,
				max: 1 * HOUR
			} },
			"wtm.attrack.keysv2": { readyIn: {
				min: 5 * MINUTE,
				max: 1 * HOUR
			} },
			"wtm.attrack.tp_events": { readyIn: {
				min: 3 * MINUTE,
				max: 30 * MINUTE
			} }
		};
		this.jobScheduler.registerHandler(ATTRACK_JOB_TYPE, async (job) => {
			const { message } = job.args;
			requireParam(message);
			requireString(message.action);
			await this.communication.send(message);
		}, {
			priority: -1e3,
			maxJobsTotal: 200,
			cooldownInMs: 2 * SECOND,
			maxAutoRetriesAfterError: 2,
			ttlInMs: 5 * DAY
		});
	}
	sendInBackground(message) {
		requireParam(message);
		requireString(message.action);
		if (this.jobScheduler.active) {
			const job = {
				type: ATTRACK_JOB_TYPE,
				args: { message },
				config: this._configOverride[message.action] || {}
			};
			this.jobScheduler.registerJob(job).catch((e) => {
				logger_default.warn("Failed to register job", job, e);
				this._sendNowInBackground(message);
			});
		} else {
			logger_default.info("jobScheduler not available. Send immediately...");
			this._sendNowInBackground(message);
		}
	}
	_sendNowInBackground(message) {
		this.communication.send(message).catch((e) => {
			logger_default.error("Failed to send message", e);
		});
	}
};
//#endregion
export { AttrackMessageHandler as default };
