import store_default from "../../../npm/hybrids/src/store.js";
import { parse } from "../../../npm/tldts-experimental/dist/es6/index.js";
//#region src/pages/settings/store/hostname.js
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
var hostname_default = {
	id: true,
	value: "",
	[store_default.connect]: {
		get: () => null,
		set: (id, model) => {
			const parsed = parse(model.value);
			if (!parsed.hostname && !parsed.isIp) throw "The value must be a valid hostname or IP address.";
			return {
				...model,
				value: parsed.hostname
			};
		}
	}
};
//#endregion
export { hostname_default as default };
