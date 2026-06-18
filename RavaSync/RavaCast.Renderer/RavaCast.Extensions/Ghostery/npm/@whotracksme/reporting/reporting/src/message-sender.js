import logger_default from "./logger.js";
//#region node_modules/@whotracksme/reporting/reporting/src/message-sender.js
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
/**
* Responsible for sending WTM reports to the servers.
* To improve anonymity, data is sent through a 3rd party
* to hide the sender's IP address.
*
* Note: There is no fundamental reason why hpnv2 could not be used
* on Mobile as well, but it is a project on its own to port it.
*/
var MessageSender = class {
	constructor({ duplicateDetector, communication, jobScheduler }) {
		this.duplicateDetector = duplicateDetector;
		this.communication = communication;
		jobScheduler.registerHandler("send-message", async (job) => {
			await this.send(job.args);
		});
	}
	async send(message) {
		logger_default.debug("Preparing to send message:", message);
		const { ok, rollback, rejectReason } = await this.duplicateDetector.trySend(message);
		if (!ok) {
			logger_default.info("Rejected by duplicate detector:", rejectReason, message);
			return;
		}
		try {
			await this.communication.send(message.body);
			logger_default.info("Successfully sent message:", message.body);
		} catch (e) {
			await rollback();
			throw e;
		}
	}
};
//#endregion
export { MessageSender as default };
