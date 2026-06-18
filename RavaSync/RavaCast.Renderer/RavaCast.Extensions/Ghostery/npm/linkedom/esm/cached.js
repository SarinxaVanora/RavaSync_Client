import { defineProperties, getOwnPropertyDescriptors } from "./shared/object.js";
import "./interface/event-target.js";
import "./interface/node-list.js";
import { Attr } from "./interface/attr.js";
import { CharacterData } from "./interface/character-data.js";
import { ParentNode } from "./mixin/parent-node.js";
import "./interface/event.js";
import { NonElementParentNode } from "./mixin/non-element-parent-node.js";
import { Element } from "./interface/element.js";
import "./shared/facades.js";
import "./html/element.js";
import "./html/template-element.js";
import "./html/html-element.js";
import "./html/script-element.js";
import "./html/frame-element.js";
import "./html/i-frame-element.js";
import "./html/object-element.js";
import "./html/head-element.js";
import "./html/body-element.js";
import "./html/style-element.js";
import "./html/time-element.js";
import "./html/field-set-element.js";
import "./html/embed-element.js";
import "./html/hr-element.js";
import "./html/progress-element.js";
import "./html/paragraph-element.js";
import "./html/table-element.js";
import "./html/frame-set-element.js";
import "./html/li-element.js";
import "./html/base-element.js";
import "./html/data-list-element.js";
import "./html/input-element.js";
import "./html/param-element.js";
import "./html/media-element.js";
import "./html/audio-element.js";
import "./html/heading-element.js";
import "./html/directory-element.js";
import "./html/quote-element.js";
import "./html/canvas-element.js";
import "./html/legend-element.js";
import "./html/option-element.js";
import "./html/span-element.js";
import "./html/meter-element.js";
import "./html/video-element.js";
import "./html/table-cell-element.js";
import "./html/title-element.js";
import "./html/output-element.js";
import "./html/table-row-element.js";
import "./html/data-element.js";
import "./html/menu-element.js";
import "./html/select-element.js";
import "./html/br-element.js";
import "./html/button-element.js";
import "./html/map-element.js";
import "./html/opt-group-element.js";
import "./html/d-list-element.js";
import "./html/text-area-element.js";
import "./html/font-element.js";
import "./html/div-element.js";
import "./html/link-element.js";
import "./html/slot-element.js";
import "./html/form-element.js";
import "./html/image-element.js";
import "./html/pre-element.js";
import "./html/u-list-element.js";
import "./html/meta-element.js";
import "./html/picture-element.js";
import "./html/area-element.js";
import "./html/o-list-element.js";
import "./html/table-caption-element.js";
import "./html/anchor-element.js";
import "./html/label-element.js";
import "./html/unknown-element.js";
import "./html/mod-element.js";
import "./html/details-element.js";
import "./html/source-element.js";
import "./html/track-element.js";
import "./html/marquee-element.js";
import "./shared/html-classes.js";
import "./interface/custom-event.js";
import "./interface/input-event.js";
import { HTMLDocument } from "./html/document.js";
import { childNodesWM, childrenWM, get, querySelectorAllWM, querySelectorWM, reset } from "./shared/cache.js";
import "./dom/parser.js";
import "./index.js";
//#region node_modules/linkedom/esm/cached.js
var { value: { get: getAttributeValue, set: setAttributeValue } } = getOwnPropertyDescriptors(Attr.prototype);
defineProperties(Attr.prototype, { value: {
	get: getAttributeValue,
	set(value) {
		reset(this.ownerElement);
		setAttributeValue.call(this, value);
	}
} });
var { remove: removeCharacterData } = CharacterData.prototype;
defineProperties(CharacterData.prototype, { remove: { value() {
	reset(this.parentNode);
	removeCharacterData.call(this);
} } });
var elementProtoDescriptors = {};
for (const name of [
	"remove",
	"setAttribute",
	"setAttributeNS",
	"setAttributeNode",
	"setAttributeNodeNS",
	"removeAttribute",
	"removeAttributeNS",
	"removeAttributeNode"
]) {
	const method = Element.prototype[name];
	elementProtoDescriptors[name] = { value() {
		reset(this.parentNode);
		return method.apply(this, arguments);
	} };
}
defineProperties(Element.prototype, elementProtoDescriptors);
var { childNodes: { get: getChildNodes }, children: { get: getChildren } } = getOwnPropertyDescriptors(ParentNode.prototype);
defineProperties(ParentNode.prototype, {
	childNodes: { get() {
		return get(childNodesWM, this, getChildNodes);
	} },
	children: { get() {
		return get(childrenWM, this, getChildren);
	} }
});
var { insertBefore, querySelector, querySelectorAll } = ParentNode.prototype;
var query = (wm, method, self, selectors) => {
	if (!wm.has(self)) wm.set(self, /* @__PURE__ */ new Map());
	const map = wm.get(self);
	if (map.has(selectors)) return map.get(selectors);
	const result = method.call(self, selectors);
	map.set(selectors, result);
	return result;
};
defineProperties(ParentNode.prototype, {
	insertBefore: { value(node, before) {
		reset(this);
		if (node.nodeType === 11) reset(node);
		return insertBefore.call(this, node, before);
	} },
	getElementsByClassName: { value(className) {
		return this.querySelectorAll("." + className);
	} },
	getElementsByTagName: { value(tagName) {
		return this.querySelectorAll(tagName);
	} },
	querySelector: { value(selectors) {
		return query(querySelectorWM, querySelector, this, selectors);
	} },
	querySelectorAll: { value(selectors) {
		return query(querySelectorAllWM, querySelectorAll, this, selectors);
	} }
});
defineProperties(NonElementParentNode.prototype, { getElementById: { value(id) {
	return this.querySelector("#" + id);
} } });
var { title: { get: getTitle, set: setTitle } } = getOwnPropertyDescriptors(HTMLDocument.prototype);
defineProperties(HTMLDocument.prototype, { title: {
	get: getTitle,
	set(value) {
		reset(this.head);
		setTitle.call(this, value);
	}
} });
//#endregion
