import logger_default from "./logger.js";
import SeqExecutor from "./seq-executor.js";
//#region node_modules/@whotracksme/reporting/reporting/src/page-aggregator.js
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
var COOLDOWN_FOR_EXPIRED_PAGES_CHECK = 2 * SECOND;
var COOLDOWN_FOR_FORCING_A_FULL_SYNC = 90 * SECOND;
var PageAggregator = class {
	constructor({ pages, pagedb, jobScheduler }) {
		this.active = false;
		this.pages = pages;
		this.pagedb = pagedb;
		this.jobScheduler = jobScheduler;
		this._dbExecutor = new SeqExecutor();
		this._lastFullSync = 0;
		this._lastExpiredPagesCheck = 0;
	}
	async init() {
		this.active = true;
	}
	unload() {
		this.active = false;
		this.pagedb.flush().catch(console.error);
	}
	onPageEvent(event) {
		if (!this.active) return;
		if (event.type === "full-sync") this.fullSync();
		else if (event.type === "lazy-init" || event.type === "page-updated") if (Date.now() > this._lastFullSync + COOLDOWN_FOR_FORCING_A_FULL_SYNC) {
			logger_default.debug("Forcing full sync");
			this.fullSync();
		} else this.syncTab(event.tabId);
		else if (event.type === "search-landing") this.syncTab(event.tabId);
		else if (event.type === "activity-updated") {
			const { urls, activityEstimator } = event;
			this._dbExecutor.run(async () => {
				await this.pagedb.updateActivity(urls, activityEstimator);
			}).catch(console.error);
		}
	}
	fullSync() {
		const updateInfo = {
			fetchPagesToUpdate: () => {
				const { openTabs, activeTab } = this.pages.describe();
				return {
					openPages: Object.values(openTabs),
					activePage: activeTab?.tab
				};
			},
			isFullSync: true
		};
		this._lastFullSync = Date.now();
		this._syncToDB(updateInfo).catch(logger_default.error);
	}
	syncTab(tabId) {
		this._syncToDB({
			fetchPagesToUpdate: () => {
				const page = this.pages.describeTab(tabId);
				if (!page) return {
					openPages: [],
					activePage: null
				};
				const activePage = this.pages.getActiveTabId() === tabId ? page : null;
				return {
					openPages: [page],
					activePage
				};
			},
			isFullSync: false
		}).catch(logger_default.error);
	}
	async _syncToDB(updateInfo) {
		await this._dbExecutor.run(async () => {
			const { openPages, activePage } = updateInfo.fetchPagesToUpdate();
			await this.pagedb.updatePages(openPages, activePage, { isFullSync: updateInfo.isFullSync });
		});
		this._expirePagesInBackground();
	}
	/**
	* For options, see PageDB#acquireExpiredPages
	*/
	async checkExpiredPages(options = {}) {
		if (!this.active) {
			logger_default.warn("checkExpiredPages ignored since the module is not active");
			return { numJobsCreated: 0 };
		}
		const now = Date.now();
		if (now < this._lastExpiredPagesCheck + COOLDOWN_FOR_EXPIRED_PAGES_CHECK && !options.forceExpiration) return { numJobsCreated: 0 };
		const defaultBatchSize = 20;
		const maxSteps = 100;
		options = {
			maxEntriesToCheck: defaultBatchSize,
			...options
		};
		let numJobsCreated = 0;
		let step;
		for (step = 0; step < maxSteps; step += 1) {
			this._lastExpiredPagesCheck = now;
			const expiredPages = await this.pagedb.acquireExpiredPages(options);
			if (expiredPages.length === 0) break;
			const jobs = expiredPages.map((page) => ({
				type: "doublefetch-page",
				args: { page }
			}));
			try {
				logger_default.debug("Creating", jobs.length, "jobs for expired pages:", jobs);
				await this.jobScheduler.registerJobs(jobs);
				numJobsCreated += expiredPages.length;
			} catch (e) {
				logger_default.error("Failed to register jobs:", jobs.length, "jobs were lost");
				logger_default.debug("Lost jobs:", jobs);
				return { numJobsCreated };
			}
		}
		if (step === maxSteps) logger_default.warn("Exceeded the iteration cap of", maxSteps, "steps (no error, but highly unexpected)");
		if (numJobsCreated > 0) logger_default.info(numJobsCreated, "jobs created for expired pages");
		return { numJobsCreated };
	}
	_expirePagesInBackground() {
		(async () => {
			try {
				await this.checkExpiredPages();
			} catch (e) {
				logger_default.error("Unexpected error while expiring pages", e);
			}
		})();
	}
};
//#endregion
export { PageAggregator as default };
