import logger_default from "./logger.js";
import SeqExecutor from "./seq-executor.js";
import { requireString, split0 } from "./utils.js";
import { DynamicDoublefetchNotSupportedError, MultiStepDoublefetchNotSupportedError, PermanentlyUnableToFetchUrlError, RateLimitedByServerError, TemporarilyUnableToFetchUrlError, UnableToOverrideHeadersError } from "./errors.js";
import { randomBetween } from "./random.js";
//#region node_modules/@whotracksme/reporting/reporting/src/http.js
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
var SECOND = 1e3;
var RESERVED_DNR_RULE_ID_HEADER_OVERRIDE = 1333;
var RESERVED_DNR_RULE_ID_OFFSCREEN = 1334;
var DNR_HEADER_OVERWRITE_PRIORITY = (1 << 30) - 1;
/**
* Depending on the context, there are different ways how requests are being
* triggered. For instance, the normal one is to use the fetch API; but
* with dynamic rendering using offscreen, it will be technically triggered
* from an iframe. That impacts how APIs should be later interpreted.
*/
var REQUEST_TYPE = {
	FETCH_API: Symbol("FETCH_API"),
	IFRAME: Symbol("IFRAME")
};
function matchesWebRequestApiType(requestType, webRequestApiType) {
	if (requestType === REQUEST_TYPE.FETCH_API) return webRequestApiType === "xmlhttprequest";
	if (requestType === REQUEST_TYPE.IFRAME) return webRequestApiType === "sub_frame";
	throw new Error(`Unexpected requestType: ${requestType}`);
}
function requestTypeToDNRResourceTypes(requestType) {
	if (requestType === REQUEST_TYPE.FETCH_API) return ["xmlhttprequest"];
	if (requestType === REQUEST_TYPE.IFRAME) return ["sub_frame"];
	throw new Error(`Unexpected requestType: ${requestType}`);
}
function getExtensionDomain() {
	getExtensionDomain.cached = getExtensionDomain.cached || new URL(chrome.runtime.getURL("")).host;
	return getExtensionDomain.cached;
}
function identicalExceptForSearchParams(url1, url2) {
	if (url1 === url2) return true;
	const x = new URL(url1);
	const y = new URL(url2);
	x.search = "";
	y.search = "";
	return x.toString() === y.toString();
}
var BUILTIN_PARAM_MODIFIERS = { 
/**
* Replaced the old value with a random one (10-20 alpha-numeric characters).
*/
random1: (oldValue) => {
	requireString(oldValue);
	const length = Math.round(randomBetween(6, 14));
	const array = new Uint8Array(length);
	crypto.getRandomValues(array);
	let result = "";
	const characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.- ";
	for (let i = 0; i < length; i++) result += characters[array[i] % 65];
	return result.trim();
} };
function createLocalUrl(url, params) {
	const paramEntries = Object.entries(params);
	if (paramEntries.length === 0) return url;
	const modifiedUrl = new URL(url);
	for (const [operation, queryKeyNames] of paramEntries) {
		const func = BUILTIN_PARAM_MODIFIERS[operation];
		if (!func) throw new Error(`Unsupported operation: <<${operation}>>`);
		const { searchParams } = modifiedUrl;
		for (const key of queryKeyNames) {
			const val = searchParams.get(key);
			if (val !== null) searchParams.set(key, func(val));
		}
	}
	return modifiedUrl.toString();
}
/**
* Note: 429 (too many requests) is intentionally not included in the list.
* Even though it is by definition a temporary error, it depends on the context
* whether it should be retried. To reflect that, it is a special case
* with its own exception class (RateLimitedByServerError) and a flag
* to "anonymousHttpGet" to override whether it should be retried or not
* ("treat429AsPermanentError").
*/
var httpStatusCodesThatShouldBeRetried = [
	500,
	502,
	503,
	504,
	599
];
async function headerOverride(params) {
	const { headers } = params;
	if (!headers || Object.keys(headers).length === 0) return () => {};
	if (chrome?.webRequest?.onBeforeSendHeaders && chrome.runtime.getManifest().permissions.includes("webRequestBlocking")) return headerOverrideViaWebRequestAPI(params);
	else if (chrome?.declarativeNetRequest?.updateSessionRules) return await headerOverrideViaDNR(params);
	else throw new UnableToOverrideHeadersError();
}
function headerOverrideViaWebRequestAPI({ url, headers, requestType }) {
	let uninstallHandler;
	let webRequestHandler = (details) => {
		if (details.url !== url || details.method !== "GET" || !matchesWebRequestApiType(requestType, details.type)) return {};
		uninstallHandler();
		const headerNames = Object.keys(headers);
		const normalizedHeaders = headerNames.map((x) => x.toLowerCase());
		details.requestHeaders = details.requestHeaders.filter((header) => !normalizedHeaders.includes(header.name.toLowerCase()));
		headerNames.forEach((name) => {
			details.requestHeaders.push({
				name,
				value: headers[name]
			});
		});
		return { requestHeaders: details.requestHeaders };
	};
	logger_default.debug("Installing temporary webrequest handler for URL:", url);
	chrome.webRequest.onBeforeSendHeaders.addListener(webRequestHandler, { urls: [url] }, ["blocking", "requestHeaders"]);
	uninstallHandler = () => {
		if (webRequestHandler) {
			logger_default.debug("Removing temporary webrequest handler for URL:", url);
			chrome.webRequest.onBeforeSendHeaders.removeListener(webRequestHandler);
			webRequestHandler = null;
		}
	};
	return uninstallHandler;
}
async function headerOverrideViaDNR({ url, headers, requestType }) {
	const rule = {
		id: RESERVED_DNR_RULE_ID_HEADER_OVERRIDE,
		priority: DNR_HEADER_OVERWRITE_PRIORITY,
		action: {
			type: "modifyHeaders",
			requestHeaders: Object.entries(headers).map(([header, value]) => ({
				header,
				operation: "set",
				value
			}))
		},
		condition: {
			urlFilter: url,
			resourceTypes: requestTypeToDNRResourceTypes(requestType),
			initiatorDomains: [getExtensionDomain()]
		}
	};
	logger_default.debug("Installing temporary DNR rule for URL:", url);
	const cleanup = await addDNRSessionRule(rule);
	return () => {
		logger_default.debug("Removing temporary DNR rule for URL:", url);
		return cleanup();
	};
}
async function addDNRSessionRule(rule) {
	try {
		await chrome.declarativeNetRequest.updateSessionRules({ addRules: [rule] });
	} catch (e) {
		try {
			logger_default.warn("Unable to install DNR rule. Trying to delete rule first:", rule, e);
			await chrome.declarativeNetRequest.updateSessionRules({ removeRuleIds: [rule.id] });
		} catch (e2) {
			logger_default.warn("Retry not possible (unable to remove DNR rule):", rule, e2);
			throw e;
		}
		logger_default.debug("Second attempt to install DNR rule:", rule);
		await chrome.declarativeNetRequest.updateSessionRules({ addRules: [rule] });
		logger_default.debug("Succeed in install DNR rule on the second attempt:", rule);
	}
	return () => {
		return chrome.declarativeNetRequest.updateSessionRules({ removeRuleIds: [rule.id] }).catch((e) => {
			logger_default.error("cleanup failed: unable to remove DNR rule:", rule, e);
		});
	};
}
async function tryCloseOffscreenDocument() {
	try {
		await chrome.offscreen.closeDocument();
	} catch (e) {
		if (e.message === "No current offscreen document.") logger_default.debug("offscreen document already closed.");
		else throw e;
	}
}
var OFFSCREEN_DOCUMENT_PREFIX = "offscreen/doublefetch";
var OFFSCREEN_DOCUMENT_INDEX_HTML = `${OFFSCREEN_DOCUMENT_PREFIX}/index.html`;
var OFFSCREEN_DOCUMENT_FIXES_JS = `${OFFSCREEN_DOCUMENT_PREFIX}/offscreen-fix.js`;
async function tryRegisterOffscreenFixes(domain) {
	if (!chrome?.scripting?.registerContentScripts) {
		logger_default.warn("Skipping offscreen-fixes (chrome.scripting API unavailable)");
		return () => {};
	}
	logger_default.debug("Installing offscreen fixes for domain:", domain);
	const id = "offscreen-fix";
	await chrome.scripting.registerContentScripts([{
		id,
		matches: [`https://*.${domain}/*`, `https://${domain}/*`],
		js: [OFFSCREEN_DOCUMENT_FIXES_JS],
		runAt: "document_start",
		allFrames: true,
		world: "MAIN"
	}]);
	return () => {
		logger_default.debug("Removing offscreen fixes for domain:", domain);
		chrome.scripting.unregisterContentScripts({ ids: [id] }).catch((e) => {
			logger_default.error("cleanup failed: unable to remove offscreen fixes content script:", e);
		});
	};
}
async function withOffscreenDocumentReady(url, headers, asyncCallback) {
	const cleanups = [];
	try {
		const domain = new URL(url).hostname;
		const undoDNRChange = await addDNRSessionRule({
			id: RESERVED_DNR_RULE_ID_OFFSCREEN,
			condition: {
				initiatorDomains: [chrome.runtime.id],
				requestDomains: [domain],
				resourceTypes: ["sub_frame"]
			},
			action: {
				type: "modifyHeaders",
				responseHeaders: [{
					header: "X-Frame-Options",
					operation: "remove"
				}, {
					header: "Frame-Options",
					operation: "remove"
				}]
			}
		});
		cleanups.push(undoDNRChange);
		const undoOffscreenFixes = await tryRegisterOffscreenFixes(domain);
		cleanups.push(undoOffscreenFixes);
		const undoHeaderOverride = await headerOverride({
			headers,
			url,
			requestType: REQUEST_TYPE.IFRAME
		});
		cleanups.push(undoHeaderOverride);
		const offscreenUrl = chrome.runtime.getURL(OFFSCREEN_DOCUMENT_INDEX_HTML);
		const existingContexts = await chrome.runtime.getContexts({
			contextTypes: ["OFFSCREEN_DOCUMENT"],
			documentUrls: [offscreenUrl]
		});
		if (existingContexts.length > 0) {
			logger_default.warn("Existing context found:", existingContexts);
			throw new Error("Unexpected: existing context found");
		}
		await chrome.offscreen.createDocument({
			url: OFFSCREEN_DOCUMENT_INDEX_HTML,
			reasons: ["IFRAME_SCRIPTING"],
			justification: "credentialless iframe"
		});
		cleanups.push(() => tryCloseOffscreenDocument());
		return await asyncCallback();
	} finally {
		await Promise.all(cleanups.map(async (x) => {
			try {
				await x();
			} catch (e) {
				logger_default.warn("Unexpected error while cleaning up resources:", e);
			}
		}));
	}
}
var EMPTY_RESCUE_HTML = `
<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8">
    <meta name="rescue" content="error">
    <title>Empty</title>
  </head>
<body></body>
</html>
`;
async function singleHttpGetStep(url, params = {}) {
	const { headers = null, redirect = "manual", shouldFollowRedirect = () => true, timeout = 15 * SECOND, treat429AsPermanentError = false, downloadLimit = 10 * 1024 * 1024, allowedContentTypes = null } = params;
	if (redirect !== "follow" && params.shouldFollowRedirect) logger_default.warn("shouldFollowRedirect ignored because redirects will be automatically resolved");
	const controller = new AbortController();
	const options = {
		credentials: "omit",
		redirect,
		signal: controller.signal
	};
	let timeoutTimer;
	let timeoutTriggered = false;
	if (timeout && timeout > 0) timeoutTimer = setTimeout(() => {
		const msg = `Request to ${url} timed out (limit: ${timeout} ms)`;
		controller.abort(new TemporarilyUnableToFetchUrlError(msg));
		logger_default.warn(msg);
		timeoutTimer = null;
		timeoutTriggered = true;
	}, timeout);
	try {
		let response;
		const undoHeaderOverride = await headerOverride({
			headers,
			url,
			requestType: REQUEST_TYPE.FETCH_API
		});
		try {
			response = await fetch(url, options);
		} catch (e) {
			throw new TemporarilyUnableToFetchUrlError(`Failed to fetch url ${url}`, { cause: e });
		} finally {
			await undoHeaderOverride();
		}
		if (response.status === 0 && (response.type == "opaqueredirect" || response.type == "opaque")) throw new PermanentlyUnableToFetchUrlError(`Failed to fetch url ${url}: not allowed to follow redirects (response.type=${response.type})`);
		if (response.url !== url && !shouldFollowRedirect(response.url)) throw new PermanentlyUnableToFetchUrlError(`Failed to fetch url ${url}: detected forbidden redirect to ${response.url}`);
		if (!response.ok) {
			const msg = `Failed to fetch url ${url}: ${response.statusText}`;
			if (response.status === 429) {
				if (treat429AsPermanentError) throw new PermanentlyUnableToFetchUrlError(msg);
				throw new RateLimitedByServerError(msg);
			}
			if (httpStatusCodesThatShouldBeRetried.includes(response.status)) throw new TemporarilyUnableToFetchUrlError(msg);
			throw new PermanentlyUnableToFetchUrlError(msg);
		}
		if (downloadLimit && downloadLimit > 0) {
			const contentLength = response.headers.get("content-length");
			if (contentLength && contentLength > downloadLimit) {
				const err = new PermanentlyUnableToFetchUrlError(`Exceeded size limit when fetching url ${url} (${contentLength} > ${downloadLimit})`);
				controller.abort(err);
				throw err;
			}
		}
		if (allowedContentTypes) {
			const value = response.headers.get("content-type");
			if (value) {
				const contentType = split0(value, ";").trim().toLowerCase();
				if (!allowedContentTypes.includes(contentType)) {
					const err = new PermanentlyUnableToFetchUrlError(`Unexpected "Content-Type" <${contentType}> (<${value}> not in {${allowedContentTypes}})`);
					controller.abort(err);
					throw err;
				}
			} else logger_default.warn("The URL", url, "did not return a \"Content-Type\" HTTP header.", "Continue and assume the types are matching...");
		}
		try {
			return await response.text();
		} catch (e) {
			if (timeoutTriggered) throw new TemporarilyUnableToFetchUrlError(`Failed to fetch url ${url} because the request timed out.`);
			throw new TemporarilyUnableToFetchUrlError(`Failed to fetch url ${url} (${e.message})`, { cause: e });
		}
	} finally {
		clearTimeout(timeoutTimer);
	}
}
var LOCK = new SeqExecutor();
/**
* Performs a HTTP Get. Given the constraints, it tries to include
* as few information as possible (e.g. credentials from cookies will
* be omitted).
*
* Optional:
* - headers: allows to overwrite headers
* - redirect: 'follow' (or by default, it will fail with a permanent error)
* - shouldFollowRedirect:
*     If redirect is set 'follow', this callback decides whether the final
*     redirect location should be followed. By default, all redirects are
*     accepted. Expects a function of type (finalUrl) => true|false
* - treat429AsPermanentError:
*     By default, HTTP 429 (too many requests) will result in a
*     recoverable error (RateLimitedByServerError). For many use cases,
*     that is reasonable; but for double-fetch requests, repeating may
*     degrade user experience. Setting this flag will instead raise a
*     PermanentlyUnableToFetchUrlError instead.
* - downloadLimit:
*     A best-effort attempt to define an upper bound for the number of
*     bytes downloaded. Note that there are no guarantees that it will
*     be able to stop bigger downloads. Also, it is based on the data
*     that comes over the wire; in other words, *after* compression.
* - allowedContentTypes (optional):
*     An optional list of supported Content-Types (e.g. "text/html").
*     If given and the HTTP "Content-Type" header does not match, the
*     function will fail with a permanent error.
* - steps (optional):
*     To configure multi-step double-fetch. That means, multiple requests
*     to the same URL can be performed; values seen in earlier request
*     can be used in later requests. Still, the chain of requests always
*     starts from a clean state. In other words, the temporary context is
*     fully isolated and will be discarded once all steps have completed.
* - onError (optional):
*     A fallback configuration for error recovery.
* - emptyHtml (default: false):
*     Bypasses the fetch and returns an empty HTML document
*     (see EMPTY_RESCUE_HTML).
*     This can sometimes be useful, especially for error recovery,
*     to gracefully handle with failed requests.
*/
async function anonymousHttpGet(originalUrl, params = {}) {
	return LOCK.run(() => anonymousHttpGet_(originalUrl, params));
}
async function anonymousHttpGet_(originalUrl, params = {}) {
	const { steps = [], emptyHtml = false, onError, ...sharedParams } = params;
	if (emptyHtml) {
		logger_default.debug("Returning empty HTML for URL:", originalUrl);
		return EMPTY_RESCUE_HTML;
	}
	try {
		if (steps.length === 0) return await singleHttpGetStep(originalUrl, sharedParams);
		if (!chrome?.webRequest?.onHeadersReceived) throw new MultiStepDoublefetchNotSupportedError();
		if (steps.some((x) => x.dynamic) && !chrome?.offscreen?.createDocument) throw new DynamicDoublefetchNotSupportedError();
		const observer = { onChange: null };
		const ctx = withContextObserver({
			cookie: /* @__PURE__ */ new Map(),
			cookie0: /* @__PURE__ */ new Map(),
			param: /* @__PURE__ */ new Map()
		}, observer);
		let content;
		for (let unsafeStepIdx = 0; unsafeStepIdx < steps.length; unsafeStepIdx += 1) {
			const stepIdx = unsafeStepIdx;
			const currentStep = steps[stepIdx];
			const nextStep = steps[stepIdx + 1];
			const localParams = {
				...sharedParams,
				...currentStep
			};
			const readyForNextStep = new Promise((resolve, reject) => {
				observer.onChange = null;
				if (!currentStep.dynamic) {
					resolve();
					return;
				}
				try {
					const graph = buildDependencyGraph(nextStep);
					if (graph.allReady) {
						resolve();
						return;
					}
					observer.onChange = graph.onChange;
					graph.onReady = resolve;
				} catch (e) {
					reject(e);
				}
			});
			if (localParams.headers) localParams.headers = replacePlaceholders(localParams.headers, ctx);
			const localUrl = createLocalUrl(originalUrl, localParams.params || {});
			if (!identicalExceptForSearchParams(originalUrl, localUrl)) throw new PermanentlyUnableToFetchUrlError(`Rejected: local URL should only change params, but got: ${originalUrl} -> ${localUrl}`);
			const { origin } = new URL(localUrl);
			const matchedUrls = [`${origin}/*`];
			const onBeforeSendHeaders = (details) => {
				if (details.tabId !== -1 || details.type !== "xmlhttprequest" && details.type !== "sub_frame") {
					logger_default.debug("onBeforeSendHeaders[ignored]:", details);
					return;
				}
				logger_default.debug("onBeforeSendHeaders[match]", details);
				for (const header of details.requestHeaders) if (header.name.toLowerCase() === "cookie") for (const line of header.value.split("\n")) {
					const start = line.indexOf("=");
					if (start > 0) {
						const key = line.slice(0, start);
						const value = split0(line.slice(start + 1), ";");
						ctx.cookie0.set(key, value);
					}
				}
				const { searchParams } = new URL(details.url);
				for (const [key, value] of [...searchParams]) ctx.param.set(key, value);
			};
			try {
				chrome.webRequest.onBeforeSendHeaders.addListener(onBeforeSendHeaders, { urls: matchedUrls }, ["requestHeaders", "extraHeaders"]);
			} catch (e) {
				chrome.webRequest.onBeforeSendHeaders.addListener(onBeforeSendHeaders, { urls: matchedUrls }, ["requestHeaders"]);
			}
			const onHeadersReceived = (details) => {
				if (details.tabId !== -1 || details.type !== "xmlhttprequest" && details.type !== "sub_frame") {
					logger_default.debug("onHeaderReceived[ignored]:", details);
					return { responseHeaders: details.responseHeaders };
				}
				logger_default.debug("onHeaderReceived[match]:", details);
				for (const header of details.responseHeaders) if (header.name.toLowerCase() === "set-cookie") for (const line of header.value.split("\n")) {
					const start = line.indexOf("=");
					if (start > 0) {
						const key = line.slice(0, start);
						const value = split0(line.slice(start + 1), ";");
						ctx.cookie.set(key, value);
					}
				}
				return { responseHeaders: details.responseHeaders };
			};
			try {
				chrome.webRequest.onHeadersReceived.addListener(onHeadersReceived, { urls: matchedUrls }, ["responseHeaders", "extraHeaders"]);
			} catch (e) {
				chrome.webRequest.onHeadersReceived.addListener(onHeadersReceived, { urls: matchedUrls }, ["responseHeaders"]);
			}
			try {
				if (currentStep.dynamic) {
					content = "";
					await withOffscreenDocumentReady(localUrl, localParams.headers, async () => {
						const { ok, error } = await chrome.runtime.sendMessage({
							target: "offscreen:urlReporting",
							type: "request",
							data: { url: localUrl }
						});
						if (!ok) throw new Error(`Unexpected error (details: ${error || "<unavailable>"})`);
						let timeout = null;
						const timedOut = new Promise((resolve, reject) => {
							const maxTime = params.timeout || 15 * SECOND;
							timeout = setTimeout(() => {
								timeout = null;
								logger_default.warn("Dynamic fetch of URL", localUrl, "exceeded the timeout of", maxTime, "ms. Details:", {
									localUrl,
									currentStep,
									nextStep,
									ctx
								});
								reject(`Timeout when dynamically fetching ${localUrl}`);
							}, maxTime);
						});
						try {
							await Promise.race([readyForNextStep, timedOut]);
						} finally {
							clearTimeout(timeout);
						}
					});
				} else content = await singleHttpGetStep(localUrl, localParams);
			} finally {
				chrome.webRequest.onBeforeSendHeaders.removeListener(onBeforeSendHeaders);
				chrome.webRequest.onHeadersReceived.removeListener(onHeadersReceived);
			}
		}
		return content;
	} catch (e) {
		if (onError) {
			const rescueParams = {
				...sharedParams,
				...onError
			};
			logger_default.info("Entering rescue path for URL:", originalUrl, "Config:", rescueParams);
			return anonymousHttpGet_(originalUrl, rescueParams);
		}
		throw e;
	}
}
function replacePlaceholders(headers, ctx) {
	return Object.fromEntries(Object.entries(headers).map(([key, value]) => {
		const parts = [];
		let pos = 0;
		while (pos < value.length) {
			const start = value.indexOf("{{", pos);
			if (start < 0) {
				parts.push(value.slice(pos));
				break;
			}
			parts.push(value.slice(pos, start));
			pos = start + 2;
			const end = value.indexOf("}}", pos);
			if (end < 0) throw new Error(`Corrupted placeholder expression: ${value}`);
			const fullExpression = value.slice(pos, end);
			let resolved;
			for (const expr of fullExpression.split("||")) {
				if (expr.startsWith("cookie:")) {
					const key = expr.slice(7);
					resolved = ctx.cookie.get(key);
				} else if (expr.startsWith("cookie0:")) {
					const key = expr.slice(8);
					resolved = ctx.cookie0.get(key);
				} else if (expr.startsWith("param:")) {
					const key = expr.slice(6);
					resolved = ctx.param.get(key);
				} else throw new Error(`Unsupported expression: stopped at <<${expr}>> (full expression: ${fullExpression})`);
				if (resolved) break;
			}
			parts.push(resolved || "");
			pos = end + 2;
		}
		return [key, parts.join("")];
	}));
}
function findPlaceholders(text) {
	const found = [];
	let pos = 0;
	for (;;) {
		const startPos = text.indexOf("{{", pos);
		if (startPos === -1) break;
		const endPos = text.indexOf("}}", startPos + 2);
		if (endPos === -1) break;
		found.push(text.slice(startPos + 2, endPos));
		pos = endPos + 2;
	}
	return found;
}
function buildDependencyGraph(nextStep) {
	const graph = {
		allReady: true,
		onChange: null,
		onReady: () => {}
	};
	if (nextStep?.headers) {
		const allTemplates = Object.values(nextStep.headers);
		if (allTemplates.length > 0) {
			const pendingExpressions = /* @__PURE__ */ new Set();
			const expressionResolvedByPlaceholder = /* @__PURE__ */ new Map();
			for (const template of allTemplates) for (const expression of findPlaceholders(template)) {
				pendingExpressions.add(expression);
				for (const placeholder of expression.split("||")) {
					const unlocks = expressionResolvedByPlaceholder.get(placeholder) || [];
					if (!unlocks.includes(expression)) unlocks.push(expression);
					expressionResolvedByPlaceholder.set(placeholder, unlocks);
				}
			}
			if (pendingExpressions.size > 0) {
				logger_default.debug("[observe-mode] waiting for:", pendingExpressions);
				graph.allReady = false;
				graph.onChange = (type, key, value) => {
					if (!graph.allReady && value) {
						const placeholder = `${type}:${key}`;
						const resolvedExpressions = expressionResolvedByPlaceholder.get(placeholder) || [];
						for (const resolved of resolvedExpressions) if (pendingExpressions.delete(resolved)) {
							logger_default.debug("[observe-mode]", placeholder, "->", value, "resolved expression", resolved);
							if (pendingExpressions.size === 0) {
								logger_default.debug("[observe-mode] all dependencies resolved");
								graph.allReady = true;
								graph.onReady();
								return;
							}
						}
					}
				};
			}
		}
	}
	return graph;
}
function withContextObserver(ctx, observer) {
	Object.keys(ctx).forEach((type) => {
		ctx[type] = new Proxy(ctx[type], { get(target, property, receiver) {
			if (property === "set") return function(key, value) {
				if (observer.onChange) observer.onChange(type, key, value);
				return target.set(key, value);
			};
			if (property === "size") return target.size;
			const value = Reflect.get(target, property, receiver);
			if (typeof value === "function") return value.bind(target);
			return value;
		} });
	});
	return ctx;
}
//#endregion
export { anonymousHttpGet };
