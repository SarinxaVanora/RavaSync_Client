import { sizeOfASCII, sizeOfBool, sizeOfByte, sizeOfUTF8 } from "./data-view.js";
import { getResourceForMime } from "../../../../@remusao/small/dist/esm/index.js";
//#region node_modules/@ghostery/adblocker/dist/esm/resources.js
/*!
* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/
function btoaPolyfill(buffer) {
	if (typeof btoa !== "undefined") return btoa(buffer);
	else if (typeof Buffer !== "undefined") return Buffer.from(buffer).toString("base64");
	return buffer;
}
function isResourceValid(resource) {
	if (resource === null) return false;
	if (typeof resource !== "object") return false;
	const { name, aliases, body, contentType } = resource;
	if (typeof name !== "string") return false;
	if (!Array.isArray(aliases) || !aliases.every((alias) => typeof alias === "string")) return false;
	if (typeof body !== "string") return false;
	if (typeof contentType !== "string") return false;
	return true;
}
function isScriptletValid(scriptlet) {
	if (scriptlet === null) return false;
	if (typeof scriptlet !== "object") return false;
	const { name, aliases, body, dependencies, executionWorld, requiresTrust } = scriptlet;
	if (typeof name !== "string") return false;
	if (!Array.isArray(aliases) || !aliases.every((alias) => typeof alias === "string")) return false;
	if (typeof body !== "string") return false;
	if (!Array.isArray(dependencies) || !dependencies.every((depencency) => typeof depencency === "string")) return false;
	if (typeof executionWorld !== "undefined" && executionWorld !== "MAIN" && executionWorld !== "ISOLATED") return false;
	if (typeof requiresTrust !== "undefined" && typeof requiresTrust !== "boolean") return false;
	return true;
}
var assembleScript = (script, dependencies = []) => [
	`if (typeof scriptletGlobals === 'undefined') { var scriptletGlobals = {}; }`,
	...dependencies,
	`(${script})(...[\`{{1}}\`,\`{{2}}\`,\`{{3}}\`,\`{{4}}\`,\`{{5}}\`,\`{{6}}\`,\`{{7}}\`,\`{{8}}\`,\`{{9}}\`,\`{{10}}\`].filter((a,i) => a !== '{{'+(i+1)+'}}').map((a) => decodeURIComponent(a)))`
].join(";");
/**
* Abstraction on top of resources.txt used for redirections as well as script
* injections. It contains logic to parse, serialize and get resources by name
* for use in the engine.
*/
var Resources = class Resources {
	static deserialize(buffer) {
		const checksum = buffer.getASCII();
		const resources = [];
		const scriptlets = [];
		for (let i = 0, numberOfResources = buffer.getUint16(); i < numberOfResources; i++) {
			const name = buffer.getASCII();
			const aliases = [];
			for (let i = 0, numberOfAliases = buffer.getUint16(); i < numberOfAliases; i++) aliases.push(buffer.getASCII());
			resources.push({
				name,
				aliases,
				body: buffer.getUTF8(),
				contentType: buffer.getASCII()
			});
		}
		for (let i = 0, numberOfScriptlets = buffer.getUint16(); i < numberOfScriptlets; i++) {
			const name = buffer.getASCII();
			const aliases = [];
			for (let i = 0, numberOfAliases = buffer.getUint16(); i < numberOfAliases; i++) aliases.push(buffer.getASCII());
			const body = buffer.getUTF8();
			const hasExecutionWorld = buffer.getBool();
			const isExecutionWorldIsolated = buffer.getBool();
			const hasRequiresTrust = buffer.getBool();
			const requiresTrust = buffer.getBool();
			const dependencies = [];
			for (let i = 0, numberOfDependencies = buffer.getUint16(); i < numberOfDependencies; i++) dependencies.push(buffer.getASCII());
			const scriptlet = {
				name,
				aliases,
				body,
				dependencies
			};
			if (hasExecutionWorld) scriptlet.executionWorld = isExecutionWorldIsolated === true ? "ISOLATED" : "MAIN";
			if (hasRequiresTrust) scriptlet.requiresTrust = requiresTrust;
			scriptlets.push(scriptlet);
		}
		return new Resources({
			checksum,
			scriptlets,
			resources
		});
	}
	static parse(data, { checksum }) {
		const distribution = JSON.parse(data);
		if (distribution === null || typeof distribution !== "object") throw new Error(`Cannot parse resources.json`);
		const { scriptlets: rawScriplets, redirects: rawResources } = distribution;
		const resources = [];
		if (Array.isArray(rawResources)) for (const redirect of rawResources) if (isResourceValid(redirect)) resources.push(redirect);
		else throw new Error(`Cannot parse redirect resource: ${JSON.stringify(redirect)}`);
		const scriptlets = [];
		if (Array.isArray(rawScriplets)) for (const scriptlet of rawScriplets) if (isScriptletValid(scriptlet)) scriptlets.push(scriptlet);
		else throw new Error(`Cannot parse scriptlet: ${JSON.stringify(scriptlet)}`);
		return new Resources({
			checksum,
			scriptlets,
			resources
		});
	}
	static copy(sourceResources) {
		const checksum = sourceResources.checksum;
		const resources = [];
		const scriptlets = [];
		for (const resource of sourceResources.resources) resources.push(structuredClone(resource));
		for (const scriptlet of sourceResources.scriptlets) scriptlets.push(structuredClone(scriptlet));
		return new this({
			checksum,
			resources,
			scriptlets
		});
	}
	constructor({ checksum = "", resources = [], scriptlets = [] } = {}) {
		this.checksum = checksum;
		this.resources = resources;
		this.scriptlets = scriptlets;
		this.scriptletsCache = /* @__PURE__ */ new Map();
		this.resourcesByName = /* @__PURE__ */ new Map();
		this.scriptletsByName = /* @__PURE__ */ new Map();
		this.updateAliases();
	}
	/**
	* In case of scriptlet or resource update, you need to clear the populated caches and mappings by calling this method.
	*/
	updateAliases() {
		this.scriptletsCache.clear();
		this.resourcesByName.clear();
		this.scriptletsByName.clear();
		for (const resource of this.resources) for (const name of [resource.name, ...resource.aliases]) {
			if (this.resourcesByName.has(name)) throw new Error(`Resource with a name or alias "${name}" already exists`);
			this.resourcesByName.set(name, resource);
		}
		for (const scriptlet of this.scriptlets) for (const name of [scriptlet.name, ...scriptlet.aliases]) {
			if (this.scriptletsByName.has(name)) throw new Error(`Scriptlet with a name or alias "${name}" already exists`);
			this.scriptletsByName.set(name, scriptlet);
		}
		for (const scriptlet of this.scriptlets) for (const dependencyName of scriptlet.dependencies) if (!this.scriptletsByName.has(dependencyName)) throw new Error(`Scriptlet with a name or alias "${scriptlet.name}" has a missing depencency "${dependencyName}"`);
	}
	getResource(name) {
		let resource = this.resourcesByName.get(name);
		if (resource === void 0) {
			const extensionIndex = name.lastIndexOf(".");
			resource = getResourceForMime(extensionIndex === -1 ? name : name.slice(extensionIndex));
		}
		const { contentType, body } = resource;
		let dataUrl;
		if (resource.contentType.indexOf(";") !== -1) dataUrl = `data:${contentType},${body}`;
		else dataUrl = `data:${contentType};base64,${btoaPolyfill(body)}`;
		return {
			filename: resource.name,
			body,
			contentType,
			dataUrl
		};
	}
	getScriptlet(name) {
		const scriptlet = this.getRawScriptlet(name);
		if (scriptlet === void 0) return this.getSurrogate(name);
		let script = this.scriptletsCache.get(scriptlet.name);
		if (script !== void 0) {
			if (script.length === 0) return;
			return script;
		}
		const dependencies = this.getScriptletDependencies(scriptlet);
		script = assembleScript(scriptlet.body, dependencies);
		this.scriptletsCache.set(scriptlet.name, script);
		return script;
	}
	getSurrogate(name) {
		const resource = this.resourcesByName.get(name.endsWith(".js") ? name : `${name}.js`);
		if (resource === void 0 || resource.contentType !== "application/javascript") return;
		return resource.body;
	}
	getScriptletCanonicalName(name) {
		return this.getRawScriptlet(name)?.name;
	}
	getRawScriptlet(name) {
		if (name.endsWith(".fn")) return;
		return this.scriptletsByName.get(name.endsWith(".js") ? name : `${name}.js`);
	}
	getScriptletDependencies(scriptlet) {
		const dependencies = /* @__PURE__ */ new Map();
		const queue = [...scriptlet.dependencies];
		while (queue.length > 0) {
			const dependencyName = queue.pop();
			if (dependencies.has(dependencyName)) continue;
			const dependency = this.scriptletsByName.get(dependencyName);
			dependencies.set(dependencyName, dependency.body);
			queue.push(...dependency.dependencies);
		}
		return Array.from(dependencies.values());
	}
	getSerializedSize() {
		let estimatedSize = sizeOfASCII(this.checksum);
		estimatedSize += 2 * sizeOfByte();
		for (const { name, aliases, body: content, contentType } of this.resources) {
			estimatedSize += sizeOfASCII(name);
			estimatedSize += aliases.reduce((state, alias) => state + sizeOfASCII(alias), 2 * sizeOfByte());
			estimatedSize += sizeOfUTF8(content);
			estimatedSize += sizeOfASCII(contentType);
		}
		estimatedSize += 2 * sizeOfByte();
		for (const { name, aliases, body: content, dependencies } of this.scriptlets) {
			estimatedSize += sizeOfASCII(name);
			estimatedSize += aliases.reduce((state, alias) => state + sizeOfASCII(alias), 2 * sizeOfByte());
			estimatedSize += sizeOfUTF8(content);
			estimatedSize += sizeOfBool();
			estimatedSize += sizeOfBool();
			estimatedSize += sizeOfBool();
			estimatedSize += sizeOfBool();
			estimatedSize += dependencies.reduce((state, dependency) => state + sizeOfASCII(dependency), 2 * sizeOfByte());
		}
		return estimatedSize;
	}
	serialize(buffer) {
		buffer.pushASCII(this.checksum);
		buffer.pushUint16(this.resources.length);
		for (const { name, aliases, body: content, contentType } of this.resources) {
			buffer.pushASCII(name);
			buffer.pushUint16(aliases.length);
			for (const alias of aliases) buffer.pushASCII(alias);
			buffer.pushUTF8(content);
			buffer.pushASCII(contentType);
		}
		buffer.pushUint16(this.scriptlets.length);
		for (const { name, aliases, body: content, dependencies, executionWorld, requiresTrust } of this.scriptlets) {
			buffer.pushASCII(name);
			buffer.pushUint16(aliases.length);
			for (const alias of aliases) buffer.pushASCII(alias);
			buffer.pushUTF8(content);
			buffer.pushBool(executionWorld !== void 0);
			buffer.pushBool(executionWorld === "ISOLATED");
			buffer.pushBool(requiresTrust !== void 0);
			buffer.pushBool(requiresTrust === true);
			buffer.pushUint16(dependencies.length);
			dependencies.forEach((dependency) => buffer.pushASCII(dependency));
		}
	}
};
//#endregion
export { Resources as default };
