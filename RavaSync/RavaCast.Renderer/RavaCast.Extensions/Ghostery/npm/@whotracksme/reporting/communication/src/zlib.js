import { pako } from "../../../../pako/dist/pako.esm.js";
//#region node_modules/@whotracksme/reporting/communication/src/zlib.js
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
var inflate = pako.inflate.bind(pako);
var deflate = pako.deflate.bind(pako);
//#endregion
export { deflate, inflate };
