import store_default from "../npm/hybrids/src/store.js";
import { getSimilarTrackers, getTracker } from "../utils/trackerdb.js";
import Organization from "./organization.js";
//#region src/store/tracker.js
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
var Tracker = {
	id: true,
	name: "",
	category: "",
	categoryDescription: "",
	organization: Organization,
	[store_default.connect]: {
		get: getTracker,
		list: ({ tracker: id }) => getSimilarTrackers(id)
	}
};
//#endregion
export { Tracker as default };
