import Matrix from "./Matrix.js";
//#region node_modules/@svgdotjs/svg.js/src/types/Point.js
var Point = class Point {
	constructor(...args) {
		this.init(...args);
	}
	clone() {
		return new Point(this);
	}
	init(x, y) {
		const base = {
			x: 0,
			y: 0
		};
		const source = Array.isArray(x) ? {
			x: x[0],
			y: x[1]
		} : typeof x === "object" ? {
			x: x.x,
			y: x.y
		} : {
			x,
			y
		};
		this.x = source.x == null ? base.x : source.x;
		this.y = source.y == null ? base.y : source.y;
		return this;
	}
	toArray() {
		return [this.x, this.y];
	}
	transform(m) {
		return this.clone().transformO(m);
	}
	transformO(m) {
		if (!Matrix.isMatrixLike(m)) m = new Matrix(m);
		const { x, y } = this;
		this.x = m.a * x + m.c * y + m.e;
		this.y = m.b * x + m.d * y + m.f;
		return this;
	}
};
function point(x, y) {
	return new Point(x, y).transformO(this.screenCTM().inverseO());
}
//#endregion
export { Point as default, point };
