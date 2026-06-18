//#region node_modules/@whotracksme/reporting/reporting/src/errors.js
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
var ExtendableError = class extends Error {
	constructor(message, options) {
		super(message, options);
		this.name = this.constructor.name;
		if (typeof Error.captureStackTrace === "function") Error.captureStackTrace(this, this.constructor);
		else this.stack = new Error(message).stack;
	}
};
var RecoverableError = class extends ExtendableError {
	constructor(message, options) {
		super(message, options);
		this.isRecoverableError = true;
		this.isPermanentError = false;
	}
};
var PermanentError = class extends ExtendableError {
	constructor(message, options) {
		super(message, options);
		this.isRecoverableError = false;
		this.isPermanentError = true;
	}
};
/**
* For jobs that are ill-formed (e.g. have missing or invalid fields).
*/
var BadJobError = class extends PermanentError {};
/**
* For job handlers that are ill-formed (e.g. have missing or invalid fields
* in their configuration).
*/
var BadJobHandlerError = class extends PermanentError {};
/**
* If trying to override HTTP headers, but the platform does not support it.
*/
var UnableToOverrideHeadersError = class extends PermanentError {};
/**
* If the platform lacks the required APIs to support multi-step doublefetch.
*/
var MultiStepDoublefetchNotSupportedError = class extends PermanentError {};
/**
* If the platform lacks the required APIs to support dynamic request doublefetch.
*/
var DynamicDoublefetchNotSupportedError = class extends PermanentError {};
/**
* Thrown when requests failed, but where the client can try to
* repeat the request without modification (e.g. timeouts will
* fall into this category).
*/
var TemporarilyUnableToFetchUrlError = class extends RecoverableError {};
/**
* Thrown when requests failed, but where the client should not
* repeat the request without modification (e.g. most 4xx errors fall
* into this category).
*/
var PermanentlyUnableToFetchUrlError = class extends PermanentError {};
/**
* For 429 (too many request) errors that should be retried.
*/
var RateLimitedByServerError = class extends TemporarilyUnableToFetchUrlError {};
/**
* Thrown if patterns are invalid. This could either be because they are
* corrupted, or if the client is too outdated.
*/
var BadPatternError = class extends PermanentError {};
/**
* Thrown when an unknown transformation builtin is referrenced.
* In most situation, it means the client is outdated.
*/
var UnsupportedTransformationError = class extends PermanentError {};
//#endregion
export { BadJobError, BadJobHandlerError, BadPatternError, DynamicDoublefetchNotSupportedError, MultiStepDoublefetchNotSupportedError, PermanentlyUnableToFetchUrlError, RateLimitedByServerError, TemporarilyUnableToFetchUrlError, UnableToOverrideHeadersError, UnsupportedTransformationError };
