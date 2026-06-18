import { saveAs } from "../../utils/files.js";
//#region src/pages/download/index.js
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
try {
	const searchParams = new URL(location.href).searchParams;
	const url = searchParams.get("url");
	const filename = searchParams.get("filename");
	if (!url || !filename) throw new Error("Missing download parameters");
	if (!url.startsWith(`blob:${location.origin}`)) throw new Error("Invalid URL");
	saveAs(url, filename);
	setTimeout(() => window.close(), 30 * 1e3);
} catch {
	window.close();
}
//#endregion
