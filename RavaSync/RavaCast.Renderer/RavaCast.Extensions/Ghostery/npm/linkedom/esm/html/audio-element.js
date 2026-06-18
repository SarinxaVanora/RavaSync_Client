import { HTMLElement } from "./element.js";
//#region node_modules/linkedom/esm/html/audio-element.js
/**
* @implements globalThis.HTMLAudioElement
*/
var HTMLAudioElement = class extends HTMLElement {
	constructor(ownerDocument, localName = "audio") {
		super(ownerDocument, localName);
	}
};
//#endregion
export { HTMLAudioElement };
