//#region node_modules/@svgdotjs/svg.js/src/animation/Queue.js
var Queue = class {
	constructor() {
		this._first = null;
		this._last = null;
	}
	first() {
		return this._first && this._first.value;
	}
	last() {
		return this._last && this._last.value;
	}
	push(value) {
		const item = typeof value.next !== "undefined" ? value : {
			value,
			next: null,
			prev: null
		};
		if (this._last) {
			item.prev = this._last;
			this._last.next = item;
			this._last = item;
		} else {
			this._last = item;
			this._first = item;
		}
		return item;
	}
	remove(item) {
		if (item.prev) item.prev.next = item.next;
		if (item.next) item.next.prev = item.prev;
		if (item === this._last) this._last = item.prev;
		if (item === this._first) this._first = item.next;
		item.prev = null;
		item.next = null;
	}
	shift() {
		const remove = this._first;
		if (!remove) return null;
		this._first = remove.next;
		if (this._first) this._first.prev = null;
		this._last = this._first ? this._last : null;
		return remove.value;
	}
};
//#endregion
export { Queue as default };
