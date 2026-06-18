import { GlobalEvent } from "./event.js";
//#region node_modules/linkedom/esm/interface/input-event.js
/* c8 ignore start */
/**
* @implements globalThis.InputEvent
*/
var InputEvent = class extends GlobalEvent {
	constructor(type, inputEventInit = {}) {
		super(type, inputEventInit);
		this.inputType = inputEventInit.inputType;
		this.data = inputEventInit.data;
		this.dataTransfer = inputEventInit.dataTransfer;
		this.isComposing = inputEventInit.isComposing || false;
		this.ranges = inputEventInit.ranges;
	}
};
/* c8 ignore stop */
//#endregion
export { InputEvent };
