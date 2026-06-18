import { delimiter } from "../modules/core/regex.js";
//#region node_modules/@svgdotjs/svg.js/src/types/SVGArray.js
var SVGArray = class extends Array {
	constructor(...args) {
		super(...args);
		this.init(...args);
	}
	clone() {
		return new this.constructor(this);
	}
	init(arr) {
		if (typeof arr === "number") return this;
		this.length = 0;
		this.push(...this.parse(arr));
		return this;
	}
	parse(array = []) {
		if (array instanceof Array) return array;
		return array.trim().split(delimiter).map(parseFloat);
	}
	toArray() {
		return Array.prototype.concat.apply([], this);
	}
	toSet() {
		return new Set(this);
	}
	toString() {
		return this.join(" ");
	}
	valueOf() {
		const ret = [];
		ret.push(...this);
		return ret;
	}
};
//#endregion
export { SVGArray as default };
