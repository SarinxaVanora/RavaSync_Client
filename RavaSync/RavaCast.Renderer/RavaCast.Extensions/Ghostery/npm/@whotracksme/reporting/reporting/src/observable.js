import logger_default from "./logger.js";
//#region node_modules/@whotracksme/reporting/reporting/src/observable.js
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
var Observable = class {
	observers = [];
	addObserver(observer) {
		if (this.observers.includes(observer)) logger_default.warn("Observer already connected. Ignoring double subscription.");
		else this.observers.push(observer);
	}
	removeObserver(observer) {
		const index = this.observers.indexOf(observer);
		if (index === -1) logger_default.warn("Observer not connected. Cannot unsubscribe.");
		else this.observers.splice(index, 1);
	}
	notifyObservers(...args) {
		this.observers.forEach((cb) => {
			try {
				cb(...args);
			} catch (e) {
				logger_default.warn("Ignore error in handler", e);
			}
		});
	}
};
//#endregion
export { Observable as default };
