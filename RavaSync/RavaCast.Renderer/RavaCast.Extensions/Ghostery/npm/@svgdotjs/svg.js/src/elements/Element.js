import { proportionalSize, writeDataToDom } from "../utils/utils.js";
import { globals } from "../utils/window.js";
import { extend, getClass, makeInstance, register, root } from "../utils/adopter.js";
import { reference } from "../modules/core/regex.js";
import { point } from "../types/Point.js";
import { ctm, screenCTM } from "../types/Matrix.js";
import { bbox, inside, rbox } from "../types/Box.js";
import List from "../types/List.js";
import SVGNumber from "../types/SVGNumber.js";
import Dom from "./Dom.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Element.js
var Element = class extends Dom {
	constructor(node, attrs) {
		super(node, attrs);
		this.dom = {};
		this.node.instance = this;
		if (node.hasAttribute("data-svgjs") || node.hasAttribute("svgjs:data")) this.setData(JSON.parse(node.getAttribute("data-svgjs")) ?? JSON.parse(node.getAttribute("svgjs:data")) ?? {});
	}
	center(x, y) {
		return this.cx(x).cy(y);
	}
	cx(x) {
		return x == null ? this.x() + this.width() / 2 : this.x(x - this.width() / 2);
	}
	cy(y) {
		return y == null ? this.y() + this.height() / 2 : this.y(y - this.height() / 2);
	}
	defs() {
		const root = this.root();
		return root && root.defs();
	}
	dmove(x, y) {
		return this.dx(x).dy(y);
	}
	dx(x = 0) {
		return this.x(new SVGNumber(x).plus(this.x()));
	}
	dy(y = 0) {
		return this.y(new SVGNumber(y).plus(this.y()));
	}
	getEventHolder() {
		return this;
	}
	height(height) {
		return this.attr("height", height);
	}
	move(x, y) {
		return this.x(x).y(y);
	}
	parents(until = this.root()) {
		const isSelector = typeof until === "string";
		if (!isSelector) until = makeInstance(until);
		const parents = new List();
		let parent = this;
		while ((parent = parent.parent()) && parent.node !== globals.document && parent.nodeName !== "#document-fragment") {
			parents.push(parent);
			if (!isSelector && parent.node === until.node) break;
			if (isSelector && parent.matches(until)) break;
			if (parent.node === this.root().node) return null;
		}
		return parents;
	}
	reference(attr) {
		attr = this.attr(attr);
		if (!attr) return null;
		const m = (attr + "").match(reference);
		return m ? makeInstance(m[1]) : null;
	}
	root() {
		const p = this.parent(getClass(root));
		return p && p.root();
	}
	setData(o) {
		this.dom = o;
		return this;
	}
	size(width, height) {
		const p = proportionalSize(this, width, height);
		return this.width(new SVGNumber(p.width)).height(new SVGNumber(p.height));
	}
	width(width) {
		return this.attr("width", width);
	}
	writeDataToDom() {
		writeDataToDom(this, this.dom);
		return super.writeDataToDom();
	}
	x(x) {
		return this.attr("x", x);
	}
	y(y) {
		return this.attr("y", y);
	}
};
extend(Element, {
	bbox,
	rbox,
	inside,
	point,
	ctm,
	screenCTM
});
register(Element, "Element");
//#endregion
export { Element as default };
