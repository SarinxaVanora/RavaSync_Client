import config_default from "./config.js";
import AnonymousCommunication from "../../npm/@whotracksme/reporting/communication/src/index.js";
import prefixedIndexedDBKeyValueStore from "./storage-indexeddb.js";
import StorageLocal from "./storage-chrome-local.js";
//#region src/background/reporting/communication.js
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
var communication_default = new AnonymousCommunication({
	config: config_default.url,
	storage: new StorageLocal("communication"),
	connectDatabase: prefixedIndexedDBKeyValueStore("communication")
});
//#endregion
export { communication_default as default };
