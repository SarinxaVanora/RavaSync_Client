import { sizeOfUTF8, sizeOfUint32Array } from "../data-view.js";
import { binLookup, hasUnicode } from "../utils.js";
import { hashHostnameBackward } from "../request.js";
import { toASCII } from "../punycode.js";
//#region node_modules/@ghostery/adblocker/dist/esm/engine/domains.js
/*!
* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/
var Domains = class Domains {
	static parse(value, { delimiter = ",", debug = false } = {}) {
		if (typeof value === "string") {
			if (value.length === 0) return;
		} else if (value.size === 0) return;
		const parts = typeof value === "string" ? value.split(delimiter) : value;
		for (const part of parts) if (part.length === 0) return;
		const entities = [];
		const notEntities = [];
		const hostnames = [];
		const notHostnames = [];
		const rawParts = [];
		for (const rawHostname of parts) {
			let hostname = rawHostname;
			if (hasUnicode(hostname)) hostname = toASCII(hostname);
			const negation = hostname.charCodeAt(0) === 126;
			const entity = hostname.charCodeAt(hostname.length - 1) === 42 && hostname.charCodeAt(hostname.length - 2) === 46;
			const start = negation ? 1 : 0;
			const end = entity ? hostname.length - 2 : hostname.length;
			const hash = hashHostnameBackward(negation === true || entity === true ? hostname.slice(start, end) : hostname);
			if (negation) {
				if (entity) notEntities.push(hash);
				else notHostnames.push(hash);
				if (debug) rawParts.push(negation ? rawHostname : `~${rawHostname}`);
			} else {
				if (entity) entities.push(hash);
				else hostnames.push(hash);
				if (debug) rawParts.push(negation ? rawHostname.slice(1) : rawHostname);
			}
		}
		return new Domains({
			entities: entities.length !== 0 ? new Uint32Array(entities).sort() : void 0,
			hostnames: hostnames.length !== 0 ? new Uint32Array(hostnames).sort() : void 0,
			notEntities: notEntities.length !== 0 ? new Uint32Array(notEntities).sort() : void 0,
			notHostnames: notHostnames.length !== 0 ? new Uint32Array(notHostnames).sort() : void 0,
			parts: debug === true ? rawParts.join(delimiter) : void 0
		});
	}
	static deserialize(buffer) {
		const optionalParts = buffer.getUint8();
		return new Domains({
			entities: (optionalParts & 1) === 1 ? buffer.getUint32Array() : void 0,
			hostnames: (optionalParts & 2) === 2 ? buffer.getUint32Array() : void 0,
			notEntities: (optionalParts & 4) === 4 ? buffer.getUint32Array() : void 0,
			notHostnames: (optionalParts & 8) === 8 ? buffer.getUint32Array() : void 0,
			parts: (optionalParts & 16) === 16 ? buffer.getUTF8() : void 0
		});
	}
	constructor({ entities, hostnames, notEntities, notHostnames, parts }) {
		this.entities = entities;
		this.hostnames = hostnames;
		this.notEntities = notEntities;
		this.notHostnames = notHostnames;
		this.parts = parts;
	}
	updateId(hash) {
		const { hostnames, entities, notHostnames, notEntities } = this;
		if (hostnames !== void 0) for (const hostname of hostnames) hash = hash * 37 ^ hostname;
		if (entities !== void 0) for (const entity of entities) hash = hash * 37 ^ entity;
		if (notHostnames !== void 0) for (const notHostname of notHostnames) hash = hash * 37 ^ notHostname;
		if (notEntities !== void 0) for (const notEntity of notEntities) hash = hash * 37 ^ notEntity;
		return hash;
	}
	serialize(buffer) {
		const index = buffer.getPos();
		buffer.pushUint8(0);
		let optionalParts = 0;
		if (this.entities !== void 0) {
			optionalParts |= 1;
			buffer.pushUint32Array(this.entities);
		}
		if (this.hostnames !== void 0) {
			optionalParts |= 2;
			buffer.pushUint32Array(this.hostnames);
		}
		if (this.notEntities !== void 0) {
			optionalParts |= 4;
			buffer.pushUint32Array(this.notEntities);
		}
		if (this.notHostnames !== void 0) {
			optionalParts |= 8;
			buffer.pushUint32Array(this.notHostnames);
		}
		if (this.parts !== void 0) {
			optionalParts |= 16;
			buffer.pushUTF8(this.parts);
		}
		buffer.setByte(index, optionalParts);
	}
	getSerializedSize() {
		let estimate = 1;
		if (this.entities !== void 0) estimate += sizeOfUint32Array(this.entities);
		if (this.hostnames !== void 0) estimate += sizeOfUint32Array(this.hostnames);
		if (this.notHostnames !== void 0) estimate += sizeOfUint32Array(this.notHostnames);
		if (this.notEntities !== void 0) estimate += sizeOfUint32Array(this.notEntities);
		if (this.parts !== void 0) estimate += sizeOfUTF8(this.parts);
		return estimate;
	}
	match(hostnameHashes, entityHashes) {
		if (this.notHostnames !== void 0) {
			for (const hash of hostnameHashes) if (binLookup(this.notHostnames, hash)) return false;
		}
		if (this.notEntities !== void 0) {
			for (const hash of entityHashes) if (binLookup(this.notEntities, hash)) return false;
		}
		if (this.hostnames !== void 0 || this.entities !== void 0) {
			if (this.hostnames !== void 0) {
				for (const hash of hostnameHashes) if (binLookup(this.hostnames, hash)) return true;
			}
			if (this.entities !== void 0) {
				for (const hash of entityHashes) if (binLookup(this.entities, hash)) return true;
			}
			return false;
		}
		return true;
	}
};
//#endregion
export { Domains };
