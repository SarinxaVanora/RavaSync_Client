import { sortCategories } from "../ui/categories.js";
import { TRACKERDB_ENGINE, get, init } from "./engines.js";
import asyncSetup from "./setup.js";
import ExtendedRequest from "./request.js";
//#region src/utils/trackerdb.js
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
var setup = asyncSetup("trackerdb", [init(TRACKERDB_ENGINE)]);
function getUnidentifiedTracker(hostname) {
	return {
		id: hostname,
		name: hostname.length > 24 ? "..." + hostname.slice(-24) : hostname,
		category: "unidentified",
		exception: hostname
	};
}
function getMetadata(request) {
	if (setup.pending) {
		console.warn("[trackerdb] TrackerDB not ready yet");
		return null;
	}
	const engine = get(TRACKERDB_ENGINE);
	if (!engine) {
		console.error("[trackerdb] TrackerDB engine not available");
		return null;
	}
	let matches = engine.getPatternMetadata(request);
	if (matches.length === 0) matches = engine.metadata.fromDomain(request.hostname);
	if (matches.length === 0) {
		if (!request.hostname || !request.blocked && !request.modified) return null;
		return getUnidentifiedTracker(request.hostname);
	}
	return getTrackers().get(matches[0].pattern.key);
}
var trackersMap = /* @__PURE__ */ new Map();
function getTrackers() {
	if (!trackersMap.size) {
		const engine = get(TRACKERDB_ENGINE);
		const categories = engine.metadata.categories.getValues();
		for (const p of engine.metadata.getPatterns()) trackersMap.set(p.key, {
			id: p.key,
			name: p.name,
			category: p.category,
			categoryDescription: categories.find((c) => c.key === p.category)?.description,
			websiteUrl: p.website_url,
			exception: p.key,
			filters: p.filters,
			domains: p.domains,
			organization: p.organization
		});
	}
	return trackersMap;
}
async function getTracker(key) {
	setup.pending && await setup.pending;
	getTrackers();
	return trackersMap.get(key);
}
async function getTrackerByURL(url) {
	setup.pending && await setup.pending;
	return getMetadata(ExtendedRequest.fromRequestDetails({ url }));
}
async function getSimilarTrackers(tracker) {
	const result = [];
	tracker = await getTracker(tracker);
	if (!tracker.organization) return result;
	trackersMap.forEach((t) => {
		if (t.organization === tracker.organization && t.id !== tracker.id) result.push(t);
	});
	return result;
}
async function getCategories() {
	setup.pending && await setup.pending;
	const engine = get(TRACKERDB_ENGINE);
	const categories = new Map(engine.metadata.categories.getValues().map(({ key, description }) => [key, {
		key,
		description,
		trackers: []
	}]));
	for (const p of getTrackers().values()) categories.get(p.category).trackers.push(p);
	return [...categories.values()].filter((c) => c.trackers.length > 0).sort(sortCategories((c) => c.key));
}
var organizations = null;
async function getOrganizations() {
	if (!organizations) {
		setup.pending && await setup.pending;
		organizations = new Map(get(TRACKERDB_ENGINE).metadata.organizations.getValues().map((org) => [org.key, {
			id: org.key,
			name: org.name,
			description: org.description,
			country: org.country,
			contact: org.privacy_contact,
			websiteUrl: org.website_url,
			privacyPolicyUrl: org.privacy_policy_url
		}]));
	}
	return organizations;
}
async function getOrganization(id) {
	return (await getOrganizations()).get(id);
}
//#endregion
export { getCategories, getMetadata, getOrganization, getOrganizations, getSimilarTrackers, getTracker, getTrackerByURL, getUnidentifiedTracker, setup };
