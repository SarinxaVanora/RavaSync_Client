import store_default from "../npm/hybrids/src/store.js";
import getBrowserInfo from "./browser-info.js";
import Options from "../store/options.js";
import { withScope } from "../npm/@sentry/core/build/esm/currentScopes.js";
import { captureException as captureException$1, setTag } from "../npm/@sentry/core/build/esm/exports.js";
import { init } from "../npm/@sentry/browser/build/npm/esm/prod/sdk.js";
import Errors from "../store/errors.js";
//#region src/utils/errors.js
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
var SAMPLE_RATE = .3;
var TAG_CRITICAL = "critical";
var { version } = chrome.runtime.getManifest();
var hostRegexp = new RegExp(new URL(chrome.runtime.getURL("/")).host, "g");
init({
	tunnel: "https://crashreporting.ghostery.net/",
	dsn: "https://05c74f55666649f0b6d671b9c37f6da1@o475874.ingest.sentry.io/6447378",
	release: `ghostery-extension@${version}`,
	debug: false,
	environment: "production",
	autoSessionTracking: false,
	defaultIntegrations: false,
	sampleRate: 1,
	beforeSend(event) {
		if (!event.tags?.ua) return null;
		if (event.tags?.[TAG_CRITICAL]) return event;
		if (Math.random() > SAMPLE_RATE) return null;
		return event;
	},
	attachStacktrace: true
});
getBrowserInfo().then(({ token }) => setTag("ua", token), () => {});
async function captureException(error, { critical = false, once = false } = {}) {
	const { terms, feedback } = await store_default.resolve(Options);
	if (!terms || !feedback || !(error instanceof Error)) return;
	if (once) {
		const id = error.message;
		if (typeof id !== "string" || !id) {
			console.warn("[errors] error has no message to identify it");
			return;
		}
		const errors = await store_default.resolve(Errors);
		if (errors.onceIds[id]) return;
		await store_default.set(errors, { onceIds: { [id]: true } });
	}
	const newError = new Error(error.message);
	newError.name = error.name;
	newError.cause = error.cause;
	newError.stack = error.stack.replace(hostRegexp, "filtered");
	withScope((scope) => {
		if (critical) scope.setTag(TAG_CRITICAL, true);
		captureException$1(newError);
	});
}
(globalThis.ghostery ??= {}).errors = { captureException };
//#endregion
export { captureException };
