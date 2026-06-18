//#region node_modules/@whotracksme/reporting/reporting/src/logger.js
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
var SUPPORTED_LOG_LEVELS = new Map([
	["debug", 1],
	["info", 2],
	["log", 2],
	["warn", 3],
	["warning", 3],
	["error", 4],
	["off", 5]
]);
var loggers = /* @__PURE__ */ new Set();
var noop = () => {};
var DEFAULT_LOG_LEVEL = "log";
var logger_default = class Logger {
	static get(name, ...options) {
		return new Logger(name, ...options);
	}
	constructor(prefix, { level = DEFAULT_LOG_LEVEL } = {}) {
		this.prefix = prefix;
		this.logLevel = level;
		this._debug = console.debug || noop;
		this._log = console.log || noop;
		this._warning = console.warn || noop;
		this._error = console.error || noop;
		if (prefix) {
			const prefix = `WTM [${this.prefix}]`;
			this._debug = this._debug.bind(null, `${prefix} debug:`);
			this._log = this._log.bind(null, `${prefix} log:`);
			this._warning = this._warning.bind(null, `${prefix} warn:`);
			this._error = this._error.bind(null, `${prefix} error:`);
		}
		loggers.add(this);
	}
	setLevel(level) {
		this.setLevel = level;
	}
	isEnabledFor(level) {
		return (SUPPORTED_LOG_LEVELS.get(level) || -1) >= SUPPORTED_LOG_LEVELS.get(this.logLevel);
	}
	get debug() {
		if (this.isEnabledFor("debug")) return this._debug;
		return noop;
	}
	get info() {
		return this.log;
	}
	get log() {
		if (this.isEnabledFor("log")) return this._log;
		return noop;
	}
	get warn() {
		return this.warning;
	}
	get warning() {
		if (this.isEnabledFor("warn")) return this._warning;
		return noop;
	}
	get error() {
		if (this.isEnabledFor("error")) return this._error;
		return noop;
	}
}.get("reporting", { level: "info" });
function setLogLevel(level, { prefix = "*" } = {}) {
	if (!SUPPORTED_LOG_LEVELS.has(level)) throw new Error(`Unknow log level '${level}'`);
	if (prefix === "*") DEFAULT_LOG_LEVEL = level;
	loggers.forEach((logger) => {
		if (prefix === "*" || prefix === logger.prefix) logger.logLevel = level;
	});
}
function describeLoggers() {
	return Object.fromEntries([...loggers].map((logger) => [logger.prefix, logger.logLevel]));
}
//#endregion
export { logger_default as default, describeLoggers, setLogLevel };
