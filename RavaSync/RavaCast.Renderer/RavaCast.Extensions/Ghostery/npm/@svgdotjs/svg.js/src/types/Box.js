import { registerMethods } from "../utils/methods.js";
import { globals } from "../utils/window.js";
import { register } from "../utils/adopter.js";
import { delimiter } from "../modules/core/regex.js";
import Point from "./Point.js";
import Matrix from "./Matrix.js";
import parser from "../modules/core/parser.js";
//#region node_modules/@svgdotjs/svg.js/src/types/Box.js
function isNulledBox(box) {
	return !box.width && !box.height && !box.x && !box.y;
}
function domContains(node) {
	return node === globals.document || (globals.document.documentElement.contains || function(node) {
		while (node.parentNode) node = node.parentNode;
		return node === globals.document;
	}).call(globals.document.documentElement, node);
}
var Box = class Box {
	constructor(...args) {
		this.init(...args);
	}
	addOffset() {
		this.x += globals.window.pageXOffset;
		this.y += globals.window.pageYOffset;
		return new Box(this);
	}
	init(source) {
		source = typeof source === "string" ? source.split(delimiter).map(parseFloat) : Array.isArray(source) ? source : typeof source === "object" ? [
			source.left != null ? source.left : source.x,
			source.top != null ? source.top : source.y,
			source.width,
			source.height
		] : arguments.length === 4 ? [].slice.call(arguments) : [
			0,
			0,
			0,
			0
		];
		this.x = source[0] || 0;
		this.y = source[1] || 0;
		this.width = this.w = source[2] || 0;
		this.height = this.h = source[3] || 0;
		this.x2 = this.x + this.w;
		this.y2 = this.y + this.h;
		this.cx = this.x + this.w / 2;
		this.cy = this.y + this.h / 2;
		return this;
	}
	isNulled() {
		return isNulledBox(this);
	}
	merge(box) {
		const x = Math.min(this.x, box.x);
		const y = Math.min(this.y, box.y);
		return new Box(x, y, Math.max(this.x + this.width, box.x + box.width) - x, Math.max(this.y + this.height, box.y + box.height) - y);
	}
	toArray() {
		return [
			this.x,
			this.y,
			this.width,
			this.height
		];
	}
	toString() {
		return this.x + " " + this.y + " " + this.width + " " + this.height;
	}
	transform(m) {
		if (!(m instanceof Matrix)) m = new Matrix(m);
		let xMin = Infinity;
		let xMax = -Infinity;
		let yMin = Infinity;
		let yMax = -Infinity;
		[
			new Point(this.x, this.y),
			new Point(this.x2, this.y),
			new Point(this.x, this.y2),
			new Point(this.x2, this.y2)
		].forEach(function(p) {
			p = p.transform(m);
			xMin = Math.min(xMin, p.x);
			xMax = Math.max(xMax, p.x);
			yMin = Math.min(yMin, p.y);
			yMax = Math.max(yMax, p.y);
		});
		return new Box(xMin, yMin, xMax - xMin, yMax - yMin);
	}
};
function getBox(el, getBBoxFn, retry) {
	let box;
	try {
		box = getBBoxFn(el.node);
		if (isNulledBox(box) && !domContains(el.node)) throw new Error("Element not in the dom");
	} catch (e) {
		box = retry(el);
	}
	return box;
}
function bbox() {
	const getBBox = (node) => node.getBBox();
	const retry = (el) => {
		try {
			const clone = el.clone().addTo(parser().svg).show();
			const box = clone.node.getBBox();
			clone.remove();
			return box;
		} catch (e) {
			throw new Error(`Getting bbox of element "${el.node.nodeName}" is not possible: ${e.toString()}`);
		}
	};
	return new Box(getBox(this, getBBox, retry));
}
function rbox(el) {
	const getRBox = (node) => node.getBoundingClientRect();
	const retry = (el) => {
		throw new Error(`Getting rbox of element "${el.node.nodeName}" is not possible`);
	};
	const rbox = new Box(getBox(this, getRBox, retry));
	if (el) return rbox.transform(el.screenCTM().inverseO());
	return rbox.addOffset();
}
function inside(x, y) {
	const box = this.bbox();
	return x > box.x && y > box.y && x < box.x + box.width && y < box.y + box.height;
}
registerMethods({ viewbox: {
	viewbox(x, y, width, height) {
		if (x == null) return new Box(this.attr("viewBox"));
		return this.attr("viewBox", new Box(x, y, width, height));
	},
	zoom(level, point) {
		let { width, height } = this.attr(["width", "height"]);
		if (!width && !height || typeof width === "string" || typeof height === "string") {
			width = this.node.clientWidth;
			height = this.node.clientHeight;
		}
		if (!width || !height) throw new Error("Impossible to get absolute width and height. Please provide an absolute width and height attribute on the zooming element");
		const v = this.viewbox();
		const zoomX = width / v.width;
		const zoomY = height / v.height;
		const zoom = Math.min(zoomX, zoomY);
		if (level == null) return zoom;
		let zoomAmount = zoom / level;
		if (zoomAmount === Infinity) zoomAmount = Number.MAX_SAFE_INTEGER / 100;
		point = point || new Point(width / 2 / zoomX + v.x, height / 2 / zoomY + v.y);
		const box = new Box(v).transform(new Matrix({
			scale: zoomAmount,
			origin: point
		}));
		return this.viewbox(box);
	}
} });
register(Box, "Box");
//#endregion
export { bbox, Box as default, inside, rbox };
