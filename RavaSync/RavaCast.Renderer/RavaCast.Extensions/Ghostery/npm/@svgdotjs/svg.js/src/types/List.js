import { extend } from "../utils/adopter.js";
//#region node_modules/@svgdotjs/svg.js/src/types/List.js
var List = class extends Array {
	constructor(arr = [], ...args) {
		super(arr, ...args);
		if (typeof arr === "number") return this;
		this.length = 0;
		this.push(...arr);
	}
};
extend([List], {
	each(fnOrMethodName, ...args) {
		if (typeof fnOrMethodName === "function") return this.map((el, i, arr) => {
			return fnOrMethodName.call(el, el, i, arr);
		});
		else return this.map((el) => {
			return el[fnOrMethodName](...args);
		});
	},
	toArray() {
		return Array.prototype.concat.apply([], this);
	}
});
var reserved = [
	"toArray",
	"constructor",
	"each"
];
List.extend = function(methods) {
	methods = methods.reduce((obj, name) => {
		if (reserved.includes(name)) return obj;
		if (name[0] === "_") return obj;
		if (name in Array.prototype) obj["$" + name] = Array.prototype[name];
		obj[name] = function(...attrs) {
			return this.each(name, ...attrs);
		};
		return obj;
	}, {});
	extend([List], methods);
};
//#endregion
export { List as default };
