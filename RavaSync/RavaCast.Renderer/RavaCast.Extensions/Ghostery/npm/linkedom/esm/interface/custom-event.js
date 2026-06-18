import { GlobalEvent } from "./event.js";
//#region node_modules/linkedom/esm/interface/custom-event.js
/* c8 ignore start */
/**
* @implements globalThis.CustomEvent
*/
var CustomEvent = class extends GlobalEvent {
	constructor(type, eventInitDict = {}) {
		super(type, eventInitDict);
		this.detail = eventInitDict.detail;
	}
};
/* c8 ignore stop */
//#endregion
export { CustomEvent };
