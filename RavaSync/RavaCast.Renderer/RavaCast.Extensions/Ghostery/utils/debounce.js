//#region src/utils/debounce.js
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
function debounce(fn, { waitFor, maxWait }) {
	let delayedTimer;
	let maxWaitTimer;
	const clear = () => {
		clearTimeout(delayedTimer);
		clearTimeout(maxWaitTimer);
		delayedTimer = void 0;
		maxWaitTimer = void 0;
	};
	let args = [];
	const run = () => {
		clear();
		try {
			fn(...args);
		} finally {
			args = [];
		}
	};
	return (...latestArgs) => {
		args = latestArgs;
		if (maxWait > 0 && maxWaitTimer === void 0) maxWaitTimer = setTimeout(run, maxWait);
		clearTimeout(delayedTimer);
		delayedTimer = setTimeout(run, waitFor);
	};
}
//#endregion
export { debounce as default };
