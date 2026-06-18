import wtm_stats_default from "../rule_resources/wtm-stats.js";
//#region src/utils/wtm-stats.js
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
function getWTMStats(domain) {
	const data = wtm_stats_default.trackers[domain];
	if (data) {
		const results = {};
		wtm_stats_default.categories.forEach(function(c, i) {
			if (data[i] > 0) results[c] = data[i];
		});
		return Object.keys(results).reduce((all, current) => [...all, ...Array(results[current]).fill(current)], []);
	}
	return [];
}
function hasWTMStats(domain) {
	return !!wtm_stats_default.trackers[domain];
}
//#endregion
export { getWTMStats, hasWTMStats };
