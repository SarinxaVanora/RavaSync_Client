import { MUTATION_OBSERVER } from "../shared/symbols.js";
//#region node_modules/linkedom/esm/interface/mutation-observer.js
var createRecord = (type, target, element, addedNodes, removedNodes, attributeName, oldValue) => ({
	type,
	target,
	addedNodes,
	removedNodes,
	attributeName,
	oldValue,
	previousSibling: element?.previousSibling || null,
	nextSibling: element?.nextSibling || null
});
var queueAttribute = (observer, target, attributeName, attributeFilter, attributeOldValue, oldValue) => {
	if (!attributeFilter || attributeFilter.includes(attributeName)) {
		const { callback, records, scheduled } = observer;
		records.push(createRecord("attributes", target, null, [], [], attributeName, attributeOldValue ? oldValue : void 0));
		if (!scheduled) {
			observer.scheduled = true;
			Promise.resolve().then(() => {
				observer.scheduled = false;
				callback(records.splice(0), observer);
			});
		}
	}
};
var attributeChangedCallback = (element, attributeName, oldValue) => {
	const { ownerDocument } = element;
	const { active, observers } = ownerDocument[MUTATION_OBSERVER];
	if (active) {
		for (const observer of observers) for (const [target, { childList, subtree, attributes, attributeFilter, attributeOldValue }] of observer.nodes) if (childList) {
			if (subtree && (target === ownerDocument || target.contains(element)) || !subtree && target.children.includes(element)) {
				queueAttribute(observer, element, attributeName, attributeFilter, attributeOldValue, oldValue);
				break;
			}
		} else if (attributes && target === element) {
			queueAttribute(observer, element, attributeName, attributeFilter, attributeOldValue, oldValue);
			break;
		}
	}
};
var moCallback = (element, parentNode) => {
	const { ownerDocument } = element;
	const { active, observers } = ownerDocument[MUTATION_OBSERVER];
	if (active) {
		for (const observer of observers) for (const [target, { subtree, childList, characterData }] of observer.nodes) if (childList) {
			if (parentNode && (target === parentNode || subtree && target.contains(parentNode)) || !parentNode && (subtree && (target === ownerDocument || target.contains(element)) || !subtree && target[characterData ? "childNodes" : "children"].includes(element))) {
				const { callback, records, scheduled } = observer;
				records.push(createRecord("childList", target, element, parentNode ? [] : [element], parentNode ? [element] : []));
				if (!scheduled) {
					observer.scheduled = true;
					Promise.resolve().then(() => {
						observer.scheduled = false;
						callback(records.splice(0), observer);
					});
				}
				break;
			}
		}
	}
};
var MutationObserverClass = class {
	constructor(ownerDocument) {
		const observers = /* @__PURE__ */ new Set();
		this.observers = observers;
		this.active = false;
		/**
		* @implements globalThis.MutationObserver
		*/
		this.class = class MutationObserver {
			constructor(callback) {
				/**
				* @private
				*/
				this.callback = callback;
				/**
				* @private
				*/
				this.nodes = /* @__PURE__ */ new Map();
				/**
				* @private
				*/
				this.records = [];
				/**
				* @private
				*/
				this.scheduled = false;
			}
			disconnect() {
				this.records.splice(0);
				this.nodes.clear();
				observers.delete(this);
				ownerDocument[MUTATION_OBSERVER].active = !!observers.size;
			}
			/**
			* @param {Element} target
			* @param {MutationObserverInit} options
			*/
			observe(target, options = {
				subtree: false,
				childList: false,
				attributes: false,
				attributeFilter: null,
				attributeOldValue: false,
				characterData: false
			}) {
				if ("attributeOldValue" in options || "attributeFilter" in options) options.attributes = true;
				options.childList = !!options.childList;
				options.subtree = !!options.subtree;
				this.nodes.set(target, options);
				observers.add(this);
				ownerDocument[MUTATION_OBSERVER].active = true;
			}
			/**
			* @returns {MutationRecord[]}
			*/
			takeRecords() {
				return this.records.splice(0);
			}
		};
	}
};
//#endregion
export { MutationObserverClass, attributeChangedCallback, moCallback };
