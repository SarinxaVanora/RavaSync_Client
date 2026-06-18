import store_default from "../npm/hybrids/src/store.js";
import { DEFAULT_REGIONS } from "../utils/regions.js";
import { isOpera, isSafari } from "../utils/browser-info.js";
import { findParentDomain } from "../utils/domains.js";
import CustomFilters from "./custom-filters.js";
import ManagedConfig from "./managed-config.js";
import Notification from "./notification.js";
//#region src/store/options.js
/**
* Ghostery Browser Extension
* https://www.ghostery.com/
*
* Copyright 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0
*/
var UPDATE_OPTIONS_ACTION_NAME = "updateOptions";
var GLOBAL_PAUSE_ID = "<all_urls>";
var MODE_DEFAULT = "default";
var ENGINES = [
	{
		name: "ads",
		key: "blockAds"
	},
	{
		name: "tracking",
		key: "blockTrackers"
	},
	{
		name: "annoyances",
		key: "blockAnnoyances"
	}
];
var LOCAL_OPTIONS = [
	"terms",
	"feedback",
	"onboarding",
	"panel",
	"sync",
	"revision",
	"filtersUpdatedAt",
	"fixesFilters",
	"whatsNewVersion"
];
var PROTECTED_OPTIONS = [
	"exceptions",
	"paused",
	"zapped"
];
var OPTIONS_VERSION = 5;
var Options = {
	mode: MODE_DEFAULT,
	blockAds: true,
	blockTrackers: true,
	blockAnnoyances: true,
	regionalFilters: {
		enabled: DEFAULT_REGIONS.length > 0,
		regions: DEFAULT_REGIONS.length ? DEFAULT_REGIONS : [String]
	},
	customFilters: {
		enabled: false,
		trustedScriptlets: false
	},
	distractions: store_default.record(false),
	autoconsent: {
		autoAction: "optOut",
		gpc: true
	},
	fixesFilters: true,
	serpTrackingPrevention: true,
	redirectProtection: {
		enabled: true,
		exceptions: store_default.record(true)
	},
	wtmSerpReport: true,
	trackerWheel: false,
	...!isSafari() ? { trackerCount: true } : {},
	pauseAssistant: true,
	terms: false,
	feedback: true,
	onboarding: false,
	panel: {
		statsType: "graph",
		notifications: true
	},
	contextMenu: true,
	theme: "",
	exceptions: store_default.record({
		global: false,
		domains: [String]
	}),
	paused: store_default.record({
		revokeAt: 0,
		assist: false,
		managed: false
	}),
	zapped: store_default.record(true),
	sync: true,
	revision: 0,
	filtersUpdatedAt: 0,
	whatsNewVersion: 0,
	[store_default.connect]: {
		async get() {
			let { options = {}, optionsVersion } = await chrome.storage.local.get(["options", "optionsVersion"]);
			if (!optionsVersion) chrome.storage.local.set({ optionsVersion: OPTIONS_VERSION });
			else if (optionsVersion < OPTIONS_VERSION) await migrate(options, optionsVersion);
			if (!isSafari() && !isOpera()) return manage(options);
			return options;
		},
		async set(_, options) {
			options = options || {};
			await chrome.storage.local.set({ options });
			await chrome.runtime.sendMessage({ action: UPDATE_OPTIONS_ACTION_NAME }).catch(() => {});
			return options;
		}
	}
};
var SYNC_OPTIONS = Object.keys(Options).filter((key) => !LOCAL_OPTIONS.includes(key));
var REPORT_OPTIONS = [...SYNC_OPTIONS.filter((key) => !PROTECTED_OPTIONS.includes(key)), "filtersUpdatedAt"];
chrome.runtime.onMessage.addListener((msg) => {
	if (msg.action === UPDATE_OPTIONS_ACTION_NAME) {
		store_default.clear(Options, false);
		store_default.get(Options);
	}
});
async function migrate(options, optionsVersion) {
	if (optionsVersion < 2) {
		if (options.paused) options.paused = options.paused.reduce((acc, { id, revokeAt }) => {
			acc[id] = { revokeAt };
			return acc;
		}, {});
		console.debug("[options] Migrated to version 2:", options);
	}
	if (optionsVersion < 3) {
		const { text } = await store_default.resolve(CustomFilters);
		if (text) options.customFilters = {
			...options.customFilters,
			enabled: true
		};
		console.debug("[options] Migrated to version 3:", options);
	}
	if (optionsVersion < 4) {
		if (options.onboarding) {
			if (options.onboarding.pinIt) await store_default.set(Notification, {
				id: "pin-it",
				shown: 1
			});
			if (options.onboarding.serpShown) await store_default.set(Notification, {
				id: "opera-serp",
				shown: options.onboarding.serpShown,
				lastShownAt: options.onboarding.serpShownAt
			});
			options.onboarding = !!options.onboarding.shown;
		}
	}
	if (optionsVersion < 5) {
		if (options.redirectProtection) options.redirectProtection.enabled = true;
	}
	await chrome.storage.local.set({
		options,
		optionsVersion: OPTIONS_VERSION
	});
	console.log(`[options] Migrated to version ${OPTIONS_VERSION} from version ${optionsVersion}...`);
}
async function manage(options) {
	const managed = await store_default.resolve(ManagedConfig);
	if (managed.disableOnboarding === true) {
		options.terms = true;
		options.onboarding = true;
	}
	if (managed.disableUserControl === true) {
		options.sync = false;
		options.paused = {};
	} else if (options.paused) {
		for (const domain of Object.keys(options.paused)) if (options.paused[domain].managed === true) delete options.paused[domain];
	}
	if (managed.disableUserAccount === true) options.sync = false;
	if (managed.disableTrackersPreview === true) options.wtmSerpReport = false;
	if (managed.trustedDomains[0] !== "<none>") {
		options.paused ||= {};
		managed.trustedDomains.forEach((domain) => {
			options.paused[domain] = {
				revokeAt: 0,
				managed: true
			};
		});
	}
	if (managed.customFilters.enabled) options.customFilters = {
		enabled: true,
		trustedScriptlets: true
	};
	return options;
}
function isGloballyPaused(options) {
	return !!options.paused[GLOBAL_PAUSE_ID];
}
async function revokeGlobalPause(options) {
	await store_default.set(options, { paused: { [GLOBAL_PAUSE_ID]: null } });
}
function getPausedDetails(options, hostname) {
	if (!hostname) throw new Error("Hostname is required to get paused details");
	if (isGloballyPaused(options)) return { revokeAt: 0 };
	switch (options.mode) {
		case MODE_DEFAULT: {
			const pausedHostname = findParentDomain(options.paused, hostname);
			return pausedHostname ? options.paused[pausedHostname] : null;
		}
		case "zap": return findParentDomain(options.zapped, hostname) ? null : { revokeAt: 0 };
		default: return null;
	}
}
//#endregion
export { ENGINES, GLOBAL_PAUSE_ID, MODE_DEFAULT, REPORT_OPTIONS, SYNC_OPTIONS, Options as default, getPausedDetails, isGloballyPaused, revokeGlobalPause };
