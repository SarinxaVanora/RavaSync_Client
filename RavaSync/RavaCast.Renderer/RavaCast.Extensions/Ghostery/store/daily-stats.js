import store_default from "../npm/hybrids/src/store.js";
import { openDB } from "../npm/idb/build/index.js";
import { registerDatabase } from "../utils/indexeddb.js";
import { getTracker } from "../utils/trackerdb.js";
import Tracker from "./tracker.js";
//#region src/store/daily-stats.js
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
var DB_NAME = registerDatabase("insights");
async function getDb() {
	if (!getDb.current) getDb.current = openDB(DB_NAME, 31, {
		async upgrade(db, oldVersion, newVersion, transaction) {
			if (oldVersion >= 20) db.deleteObjectStore("search");
			if (oldVersion === 30) db.deleteObjectStore("tabs");
			if (oldVersion < 31) (oldVersion < 1 ? db.createObjectStore("daily", { keyPath: "day" }) : transaction.objectStore("daily")).createIndex("day", "day", { unique: true });
		},
		async blocking() {
			(await getDb.current).close();
			getDb.current = null;
		}
	});
	return await getDb.current;
}
var flushes = /* @__PURE__ */ new Map();
async function flush(id) {
	clearTimeout(flushes.get(id));
	flushes.set(id, setTimeout(async () => {
		try {
			const values = await store_default.get(DailyStats, id);
			await (await getDb()).put("daily", values);
		} catch (e) {
			console.error(`[daily-stats] Error while flushing daily stats`, e);
		}
		flushes.delete(id);
	}, 1e3));
}
var DailyStats = {
	id: true,
	day: "",
	trackersBlocked: 0,
	trackersModified: 0,
	cookiesRemoved: 0,
	pages: 0,
	patterns: [String],
	[store_default.connect]: {
		loose: true,
		async get(id) {
			return await (await getDb()).get("daily", id) || {
				id,
				day: id
			};
		},
		set(id, values) {
			flush(id);
			return values;
		},
		async list({ dateFrom, dateTo }) {
			return (await getDb()).getAllFromIndex("daily", "day", IDBKeyRange.bound(dateFrom, dateTo));
		}
	}
};
var MergedStats = {
	id: true,
	pages: 0,
	trackersBlocked: 0,
	trackersModified: 0,
	cookiesRemoved: 0,
	trackers: [String],
	groupedTrackers: [Tracker],
	categories: [String],
	groupedCategories: [{
		id: true,
		count: 0
	}],
	[store_default.connect]: {
		cache: false,
		async get({ dateFrom, dateTo }) {
			const data = (await store_default.resolve([DailyStats], {
				dateFrom,
				dateTo
			})).reduce((acc, stats) => {
				for (const id of stats.patterns) acc.trackers.push(id);
				acc.pages += stats.pages;
				acc.trackersBlocked += stats.trackersBlocked;
				acc.trackersModified += stats.trackersModified;
				acc.cookiesRemoved += stats.cookiesRemoved || 0;
				return acc;
			}, {
				pages: 0,
				trackers: [],
				trackersBlocked: 0,
				trackersModified: 0,
				cookiesRemoved: 0,
				categories: []
			});
			const groupedCategories = {};
			const groupedTrackers = /* @__PURE__ */ new Map();
			for (const id of data.trackers) {
				const tracker = await getTracker(id);
				const category = tracker?.category || "unidentified";
				groupedCategories[category] = (groupedCategories[category] || 0) + 1;
				if (tracker) groupedTrackers.set(tracker, (groupedTrackers.get(tracker) || 0) + 1);
			}
			data.groupedTrackers = Array.from(groupedTrackers.entries()).sort((a, b) => b[1] - a[1]).map(([tracker]) => tracker);
			data.groupedCategories = Object.entries(groupedCategories).sort((a, b) => b[1] - a[1]).map(([id, count]) => ({
				id,
				count
			}));
			if (data.groupedCategories.length > 5) {
				const other = data.groupedCategories.splice(5);
				data.groupedCategories.push({
					id: "other",
					count: other.reduce((acc, { count }) => acc + count, 0)
				});
			}
			data.categories = data.groupedCategories.reduce((acc, { id, count }) => {
				for (let i = 0; i < count; i++) acc.push(id);
				return acc;
			}, []);
			return data;
		}
	}
};
//#endregion
export { MergedStats, DailyStats as default };
