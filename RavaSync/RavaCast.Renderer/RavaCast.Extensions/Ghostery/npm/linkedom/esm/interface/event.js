//#region node_modules/linkedom/esm/interface/event.js
/* c8 ignore start */
var BUBBLING_PHASE = 3;
var AT_TARGET = 2;
var CAPTURING_PHASE = 1;
var NONE = 0;
function getCurrentTarget(ev) {
	return ev.currentTarget;
}
/**
* @implements globalThis.Event
*/
var GlobalEvent = class {
	static get BUBBLING_PHASE() {
		return BUBBLING_PHASE;
	}
	static get AT_TARGET() {
		return AT_TARGET;
	}
	static get CAPTURING_PHASE() {
		return CAPTURING_PHASE;
	}
	static get NONE() {
		return NONE;
	}
	constructor(type, eventInitDict = {}) {
		this.type = type;
		this.bubbles = !!eventInitDict.bubbles;
		this.cancelBubble = false;
		this._stopImmediatePropagationFlag = false;
		this.cancelable = !!eventInitDict.cancelable;
		this.eventPhase = this.NONE;
		this.timeStamp = Date.now();
		this.defaultPrevented = false;
		this.originalTarget = null;
		this.returnValue = null;
		this.srcElement = null;
		this.target = null;
		this._path = [];
	}
	get BUBBLING_PHASE() {
		return BUBBLING_PHASE;
	}
	get AT_TARGET() {
		return AT_TARGET;
	}
	get CAPTURING_PHASE() {
		return CAPTURING_PHASE;
	}
	get NONE() {
		return NONE;
	}
	preventDefault() {
		this.defaultPrevented = true;
	}
	composedPath() {
		return this._path.map(getCurrentTarget);
	}
	stopPropagation() {
		this.cancelBubble = true;
	}
	stopImmediatePropagation() {
		this.stopPropagation();
		this._stopImmediatePropagationFlag = true;
	}
};
/* c8 ignore stop */
//#endregion
export { GlobalEvent };
