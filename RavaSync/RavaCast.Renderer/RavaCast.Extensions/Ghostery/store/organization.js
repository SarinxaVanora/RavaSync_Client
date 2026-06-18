import store_default from "../npm/hybrids/src/store.js";
import { getOrganization } from "../utils/trackerdb.js";
//#region src/store/organization.js
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
var Organization = {
	id: true,
	name: "",
	description: "",
	country: "",
	contact: "",
	websiteUrl: "",
	privacyPolicyUrl: "",
	[store_default.connect]: getOrganization
};
//#endregion
export { Organization as default };
