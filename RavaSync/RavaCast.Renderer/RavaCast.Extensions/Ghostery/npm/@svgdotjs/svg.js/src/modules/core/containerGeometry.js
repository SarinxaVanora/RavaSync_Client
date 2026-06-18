import { __exportAll } from "../../../../../../virtual/_rolldown/runtime.js";
import { proportionalSize } from "../../utils/utils.js";
import { getWindow } from "../../utils/window.js";
import Point from "../../types/Point.js";
import Matrix from "../../types/Matrix.js";
import Box from "../../types/Box.js";
//#region node_modules/@svgdotjs/svg.js/src/modules/core/containerGeometry.js
var containerGeometry_exports = /* @__PURE__ */ __exportAll({
	dmove: () => dmove,
	dx: () => dx,
	dy: () => dy,
	height: () => height,
	move: () => move,
	size: () => size,
	width: () => width,
	x: () => x,
	y: () => y
});
function dmove(dx, dy) {
	this.children().forEach((child) => {
		let bbox;
		try {
			bbox = child.node instanceof getWindow().SVGSVGElement ? new Box(child.attr([
				"x",
				"y",
				"width",
				"height"
			])) : child.bbox();
		} catch (e) {
			return;
		}
		const m = new Matrix(child);
		const matrix = m.translate(dx, dy).transform(m.inverse());
		const p = new Point(bbox.x, bbox.y).transform(matrix);
		child.move(p.x, p.y);
	});
	return this;
}
function dx(dx) {
	return this.dmove(dx, 0);
}
function dy(dy) {
	return this.dmove(0, dy);
}
function height(height, box = this.bbox()) {
	if (height == null) return box.height;
	return this.size(box.width, height, box);
}
function move(x = 0, y = 0, box = this.bbox()) {
	const dx = x - box.x;
	const dy = y - box.y;
	return this.dmove(dx, dy);
}
function size(width, height, box = this.bbox()) {
	const p = proportionalSize(this, width, height, box);
	const scaleX = p.width / box.width;
	const scaleY = p.height / box.height;
	this.children().forEach((child) => {
		const o = new Point(box).transform(new Matrix(child).inverse());
		child.scale(scaleX, scaleY, o.x, o.y);
	});
	return this;
}
function width(width, box = this.bbox()) {
	if (width == null) return box.width;
	return this.size(width, box.height, box);
}
function x(x, box = this.bbox()) {
	if (x == null) return box.x;
	return this.move(x, box.y, box);
}
function y(y, box = this.bbox()) {
	if (y == null) return box.y;
	return this.move(box.x, y, box);
}
//#endregion
export { containerGeometry_exports };
