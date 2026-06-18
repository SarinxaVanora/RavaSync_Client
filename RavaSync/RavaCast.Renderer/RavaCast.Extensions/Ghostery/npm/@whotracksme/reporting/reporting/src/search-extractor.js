import logger_default from "./logger.js";
import { fastHash } from "./utils.js";
import { BadPatternError } from "./errors.js";
import { lookupBuiltinTransform } from "./patterns.js";
import random from "./random.js";
import { timezoneAgnosticDailyExpireAt } from "./cooldowns.js";
import { anonymousHttpGet } from "./http.js";
import parseHtml from "./html-parser.js";
//#region node_modules/@whotracksme/reporting/reporting/src/search-extractor.js
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
function doublefetchQueryHash(query, category) {
	return fastHash(`dfq:${category}:${query.trim()}`, { truncate: true });
}
function runSelector(item, selector, attr, baseURI) {
	const elem = selector ? item.querySelector(selector) : item;
	if (elem) {
		if (attr === "textContent") return elem.textContent;
		if (attr === "href") {
			const rawLink = elem.getAttribute("href");
			return rawLink ? new URL(rawLink, baseURI).href : null;
		}
		if (elem.hasAttribute(attr)) return elem.getAttribute(attr);
	}
	return null;
}
function runTransforms(value, transformSteps = []) {
	if (!Array.isArray(transformSteps)) throw new BadPatternError("Transform definitions must be an array (of arrays).");
	if (value === void 0 || value === null) return null;
	let tmpValue = value;
	for (const step of transformSteps) {
		if (!Array.isArray(step)) throw new BadPatternError("Single transform definitions must be an array.");
		const [name, ...args] = step;
		tmpValue = lookupBuiltinTransform(name)(tmpValue, ...args);
	}
	return tmpValue ?? null;
}
function findFirstMatch(rootItem, selectorDef, baseURI) {
	if (selectorDef.firstMatch) {
		for (const { select, attr, transform = [] } of selectorDef.firstMatch) {
			const match = runSelector(rootItem, select, attr, baseURI) ?? null;
			if (match !== null) return runTransforms(match, transform);
		}
		return null;
	}
	return runSelector(rootItem, selectorDef.select, selectorDef.attr, baseURI) ?? null;
}
var SearchExtractor = class {
	constructor({ patterns, sanitizer, persistedHashes, jobScheduler }) {
		this.patterns = patterns;
		this.sanitizer = sanitizer;
		this.persistedHashes = persistedHashes;
		jobScheduler.registerHandler("doublefetch-query", async (job) => {
			const { messages } = await this.runJob(job.args);
			return messages.map((message) => ({
				type: "send-message",
				args: message
			}));
		});
	}
	async runJob({ query, category, doublefetchRequest }) {
		function discard(reason = "") {
			logger_default.debug("No messages found for query:", query, "Reason:", reason);
			return {
				messages: [],
				reason
			};
		}
		const queryCheck = this.sanitizer.checkSuspiciousQuery(query);
		if (!queryCheck.accept) return discard(`Dropping suspicious query before double-fetch (${queryCheck.reason})`);
		const queryHash = doublefetchQueryHash(query, category);
		const expireAt = timezoneAgnosticDailyExpireAt();
		if (!await this.persistedHashes.add(queryHash, expireAt)) return discard("Query has been recently seen.");
		let doc;
		try {
			const { url, ...params } = doublefetchRequest;
			doc = await parseHtml(await anonymousHttpGet(url, {
				...params,
				treat429AsPermanentError: true
			}));
		} catch (e) {
			logger_default.info("Failed to fetch query:", doublefetchRequest.url, e);
			await this.persistedHashes.delete(queryHash).catch(() => {});
			throw e;
		}
		try {
			const messages = this.extractMessages({
				doc,
				query,
				category,
				doublefetchRequest
			});
			if (messages.length === 0) return discard("No content found.");
			return { messages };
		} catch (e) {
			logger_default.warn("Processing failed:", e);
			return discard(`Unsupported pattern: ${e}`);
		}
	}
	extractMessages({ doc, query, category, doublefetchRequest }) {
		const rules = this.patterns.getRulesSnapshot();
		if (!rules[category]) return [];
		const found = {};
		const baseURI = doublefetchRequest.url;
		const { preprocess = {}, input = {}, output = {} } = rules[category];
		for (const rule of preprocess.prune || []) if (rule?.first) doc.querySelector(rule.first)?.remove();
		else if (rule?.all) for (const elem of doc.querySelectorAll(rule.all) || []) elem.remove();
		else throw new BadPatternError("Bad prune rule (expected \"first\" or \"all\")");
		for (const [selector, selectorDef] of Object.entries(input)) {
			found[selector] = found[selector] || {};
			if (selectorDef.first) {
				const item = doc.querySelector(selector);
				if (item) for (const [key, def] of Object.entries(selectorDef.first)) {
					const value = findFirstMatch(item, def, baseURI);
					found[selector][key] = runTransforms(value, def.transform);
				}
			} else if (selectorDef.all) {
				const rootItems = doc.querySelectorAll(selector);
				if (rootItems) {
					found[selector] = found[selector] || {};
					for (const [key, def] of Object.entries(selectorDef.all)) {
						found[selector][key] = [];
						for (const rootItem of rootItems) {
							const item = findFirstMatch(rootItem, def, baseURI);
							found[selector][key].push(runTransforms(item, def.transform));
						}
					}
				}
			} else throw new BadPatternError("Bad selector (expected \"first\" or \"all\")");
		}
		const context = {
			q: query ?? null,
			qurl: doublefetchRequest.url,
			ctry: this.sanitizer.getSafeCountryCode()
		};
		const isPresent = (x) => x !== null && x !== void 0 && x !== "";
		const messages = [];
		nextaction: for (const [action, schema] of Object.entries(output)) {
			const payload = {};
			for (const { key, source, requiredKeys, optional = false } of schema.fields) if (source) {
				if (!input[source]) throw new BadPatternError(`Output rule for action=${action} references invalid input source=${source}`);
				if (input[source].first) {
					if (!optional && !isPresent(found[source][key])) continue nextaction;
					payload[key] = found[source][key] ?? null;
				} else if (input[source].all) {
					const results = [];
					const innerKeys = Object.keys(input[source].all);
					for (const innerKey of innerKeys) found[source][innerKey].forEach((value, idx) => {
						results[idx] = results[idx] || {};
						results[idx][innerKey] = value ?? null;
					});
					const required = requiredKeys || innerKeys;
					const allFieldsPresent = (entry) => required.every((x) => isPresent(entry[x]));
					const cleanedResults = results.filter(allFieldsPresent);
					if (cleanedResults.length === 0 && !optional) continue nextaction;
					payload[key] = { ...cleanedResults };
				} else throw new BadPatternError(`Output rule for action=${action} does not match input key=${key}`);
			} else {
				if (!optional && !isPresent(context[key])) continue;
				payload[key] = context[key] ?? null;
			}
			const { deduplicateBy } = schema;
			const body = {
				action,
				payload,
				ver: 4,
				"anti-duplicates": Math.floor(random() * 1e7)
			};
			messages.push({
				body,
				deduplicateBy
			});
		}
		logger_default.debug("Found the following messages:", messages);
		const filteredMessages = messages.filter((msg) => {
			const { omitIfExistsAny = [] } = output[msg.body.action];
			return !omitIfExistsAny.some((action) => messages.some((x) => x.body.action === action));
		});
		if (messages.length !== filteredMessages.length) logger_default.debug("Remaining messages after filtering:", filteredMessages);
		return filteredMessages;
	}
};
//#endregion
export { SearchExtractor as default };
