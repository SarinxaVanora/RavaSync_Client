import { Smaz } from "../../../../@remusao/smaz/dist/esm/index.js";
import cosmetic_selector_default from "./codebooks/cosmetic-selector.js";
import network_csp_default from "./codebooks/network-csp.js";
import network_filter_default from "./codebooks/network-filter.js";
import network_hostname_default from "./codebooks/network-hostname.js";
import network_redirect_default from "./codebooks/network-redirect.js";
import raw_network_default from "./codebooks/raw-network.js";
import raw_cosmetic_default from "./codebooks/raw-cosmetic.js";
//#region node_modules/@ghostery/adblocker/dist/esm/compression.js
/*!
* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/
var Compression = class {
	constructor() {
		this.cosmeticSelector = new Smaz(cosmetic_selector_default);
		this.networkCSP = new Smaz(network_csp_default);
		this.networkRedirect = new Smaz(network_redirect_default);
		this.networkHostname = new Smaz(network_hostname_default);
		this.networkFilter = new Smaz(network_filter_default);
		this.networkRaw = new Smaz(raw_network_default);
		this.cosmeticRaw = new Smaz(raw_cosmetic_default, 8e5);
	}
};
//#endregion
export { Compression as default };
