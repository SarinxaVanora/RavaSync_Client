import { parseWithCache } from "../../utils/request.js";
//#region src/background/adblocker/ancestors.js
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
var FramesHierarchy = class {
	/**
	* @type {Array<{ id: number; frames: Array<{ id: number; parent: number; documentId: string; details: unknown }> }>}
	*/
	tabs = [];
	#findTab(tabId) {
		return this.tabs.findIndex((tab) => tab.id === tabId);
	}
	ancestors(target, details) {
		let { tabId, frameId, parentFrameId, documentId = "" } = target;
		const tabIndex = this.#findTab(tabId);
		if (tabIndex === -1) {
			if (parentFrameId === -1) this.tabs.push({
				id: tabId,
				frames: [{
					id: frameId,
					parent: parentFrameId,
					documentId,
					details
				}]
			});
			return [];
		}
		const frames = this.tabs[tabIndex].frames;
		const chain = [];
		let frameIndex = frames.length;
		while (--frameIndex >= 0) if (frames[frameIndex].id === frameId) {
			const targetFrame = frames[frameIndex];
			if (frameId === 0) {
				targetFrame.id = -1;
				this.unregister(tabId, 0);
				targetFrame.id = 0;
			}
			targetFrame.parent = parentFrameId;
			targetFrame.documentId = documentId;
			targetFrame.details = details;
			break;
		} else if (documentId.length && frames[frameIndex].documentId === documentId) {
			this.#handleFrameReplacement(frames, frameIndex, frameId, parentFrameId, details, tabId);
			break;
		}
		if (frameIndex === -1) frames.push({
			id: frameId,
			parent: parentFrameId,
			documentId,
			details
		});
		if (parentFrameId === -1) return [];
		frameIndex = 0;
		while (frameIndex !== -1) {
			frameIndex = frames.length;
			while (--frameIndex >= 0) if (frames[frameIndex].id === parentFrameId) {
				chain.push(frames[frameIndex].details);
				parentFrameId = frames[frameIndex].parent;
				if (parentFrameId === -1) return chain;
				break;
			}
		}
		this.tabs.splice(tabIndex, 1);
		return [];
	}
	#handleFrameReplacement(frames, frameIndex, frameId, parentFrameId, details, tabId) {
		const targetFrame = frames[frameIndex];
		targetFrame.parent = -1;
		this.unregister(tabId, frameId);
		for (const frame of frames) if (frame.parent === targetFrame.id) frame.parent = frameId;
		targetFrame.id = frameId;
		targetFrame.parent = parentFrameId;
		targetFrame.details = details;
	}
	replace(addedTabId, removedTabId) {
		const tabIndex = this.#findTab(removedTabId);
		if (tabIndex !== -1) this.tabs[tabIndex].id = addedTabId;
	}
	unregister(tabId, frameId) {
		const tabIndex = this.#findTab(tabId);
		if (tabIndex === -1) return;
		const frames = this.tabs[tabIndex].frames;
		const parents = [frameId];
		while (parents.length) {
			const parent = parents.pop();
			let frameIndex = frames.length;
			while (frameIndex--) if (frames[frameIndex].parent === parent || frames[frameIndex].id === parent) {
				parents.push(frames[frameIndex].id);
				frames.splice(frameIndex, 1);
			}
		}
		if (frames.length === 0) this.tabs.splice(tabIndex, 1);
	}
	sync(tabId, frames) {
		const tabIndex = this.#findTab(tabId);
		const newFrameList = frames.map((frame) => ({
			id: frame.frameId,
			parent: frame.parentFrameId,
			documentId: frame.documentId || "",
			details: frame._details
		}));
		if (tabIndex === -1) {
			this.tabs.push({
				id: tabId,
				frames: newFrameList
			});
			return;
		}
		this.tabs[tabIndex].frames = newFrameList;
	}
	async #handleTab(tab) {
		if (!tab.id) return;
		let frames;
		try {
			frames = await chrome.webNavigation.getAllFrames({ tabId: tab.id });
		} catch (error) {
			console.error(`Failed to get frames of the tab: tabId="${tab.id}"`, error);
			return;
		}
		this.sync(tab.id, frames.map((frame) => {
			const parsed = parseWithCache(frame.url);
			return {
				frameId: frame.frameId,
				parentFrameId: frame.parentFrameId,
				documentId: frame.documentId || "",
				_details: {
					hostname: parsed.hostname || "",
					domain: parsed.domain || ""
				}
			};
		}));
	}
	async handleWebWorkerStart() {
		await Promise.all((await chrome.tabs.query({})).map((tab) => this.#handleTab(tab)));
	}
	handleWebextensionEvents() {
		chrome.tabs.onRemoved.addListener((tabId) => {
			this.unregister(tabId, 0);
		});
		chrome.tabs.onReplaced.addListener((addedTabId, removedTabId) => {
			this.replace(addedTabId, removedTabId);
		});
	}
};
//#endregion
export { FramesHierarchy };
