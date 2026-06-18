//#region node_modules/@whotracksme/reporting/reporting/src/LRU.js
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
function newNode(key, value) {
	return {
		prev: null,
		next: null,
		key,
		value
	};
}
var LRU = class {
	reset() {
		this.cache.clear();
		this.head = null;
		this.tail = null;
		this.size = 0;
	}
	constructor(size) {
		this.cache = /* @__PURE__ */ new Map();
		this.maxSize = size;
		this.reset();
	}
	has(key) {
		return this.cache.has(key);
	}
	get(key) {
		const node = this.cache.get(key);
		if (node !== void 0) {
			this.touch(node);
			return node.value;
		}
	}
	set(key, value) {
		let node = this.cache.get(key);
		if (node !== void 0) {
			node.value = value;
			this.touch(node);
		} else {
			node = newNode(key, value);
			if (this.size >= this.maxSize && this.tail !== null) {
				this.cache.delete(this.tail.key);
				this.remove(this.tail);
			}
			this.cache.set(key, node);
			this.pushFront(node);
		}
	}
	toMap() {
		return new Map(Array.from(this.cache.values()).map(({ key, value }) => [key, value]));
	}
	touch(node) {
		this.remove(node);
		this.pushFront(node);
	}
	remove(node) {
		if (node !== null) {
			if (node.prev === null) this.head = node.next;
			else node.prev.next = node.next;
			if (node.next === null) this.tail = node.prev;
			else node.next.prev = node.prev;
			this.size -= 1;
		}
	}
	pushFront(node) {
		node.prev = null;
		node.next = this.head;
		if (this.head !== null) this.head.prev = node;
		this.head = node;
		if (this.tail === null) this.tail = node;
		this.size += 1;
	}
};
//#endregion
export { LRU as default };
