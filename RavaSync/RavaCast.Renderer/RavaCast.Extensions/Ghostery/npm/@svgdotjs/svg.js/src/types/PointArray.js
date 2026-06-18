import { delimiter } from "../modules/core/regex.js";
import Matrix from "./Matrix.js";
import Box from "./Box.js";
import SVGArray from "./SVGArray.js";
//#region node_modules/@svgdotjs/svg.js/src/types/PointArray.js
var PointArray = class extends SVGArray {
	bbox() {
		let maxX = -Infinity;
		let maxY = -Infinity;
		let minX = Infinity;
		let minY = Infinity;
		this.forEach(function(el) {
			maxX = Math.max(el[0], maxX);
			maxY = Math.max(el[1], maxY);
			minX = Math.min(el[0], minX);
			minY = Math.min(el[1], minY);
		});
		return new Box(minX, minY, maxX - minX, maxY - minY);
	}
	move(x, y) {
		const box = this.bbox();
		x -= box.x;
		y -= box.y;
		if (!isNaN(x) && !isNaN(y)) for (let i = this.length - 1; i >= 0; i--) this[i] = [this[i][0] + x, this[i][1] + y];
		return this;
	}
	parse(array = [0, 0]) {
		const points = [];
		if (array instanceof Array) array = Array.prototype.concat.apply([], array);
		else array = array.trim().split(delimiter).map(parseFloat);
		if (array.length % 2 !== 0) array.pop();
		for (let i = 0, len = array.length; i < len; i = i + 2) points.push([array[i], array[i + 1]]);
		return points;
	}
	size(width, height) {
		let i;
		const box = this.bbox();
		for (i = this.length - 1; i >= 0; i--) {
			if (box.width) this[i][0] = (this[i][0] - box.x) * width / box.width + box.x;
			if (box.height) this[i][1] = (this[i][1] - box.y) * height / box.height + box.y;
		}
		return this;
	}
	toLine() {
		return {
			x1: this[0][0],
			y1: this[0][1],
			x2: this[1][0],
			y2: this[1][1]
		};
	}
	toString() {
		const array = [];
		for (let i = 0, il = this.length; i < il; i++) array.push(this[i].join(","));
		return array.join(" ");
	}
	transform(m) {
		return this.clone().transformO(m);
	}
	transformO(m) {
		if (!Matrix.isMatrixLike(m)) m = new Matrix(m);
		for (let i = this.length; i--;) {
			const [x, y] = this[i];
			this[i][0] = m.a * x + m.c * y + m.e;
			this[i][1] = m.b * x + m.d * y + m.f;
		}
		return this;
	}
};
//#endregion
export { PointArray as default };
