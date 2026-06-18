import RelativeTimeElement from "./relative-time-element.js";
//#region node_modules/@github/relative-time-element/dist/relative-time-element-define.js
var root = typeof globalThis !== "undefined" ? globalThis : window;
try {
	root.RelativeTimeElement = RelativeTimeElement.define();
} catch (e) {
	if (!(root.DOMException && e instanceof DOMException && e.name === "NotSupportedError") && !(e instanceof ReferenceError)) throw e;
}
//#endregion
