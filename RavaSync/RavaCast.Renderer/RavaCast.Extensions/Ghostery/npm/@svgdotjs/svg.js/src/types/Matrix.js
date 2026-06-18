import { radians } from "../utils/utils.js";
import { register } from "../utils/adopter.js";
import { delimiter } from "../modules/core/regex.js";
import Point from "./Point.js";
import Element from "../elements/Element.js";
//#region node_modules/@svgdotjs/svg.js/src/types/Matrix.js
function closeEnough(a, b, threshold) {
	return Math.abs(b - a) < (threshold || 1e-6);
}
var Matrix = class Matrix {
	constructor(...args) {
		this.init(...args);
	}
	static formatTransforms(o) {
		const flipBoth = o.flip === "both" || o.flip === true;
		const flipX = o.flip && (flipBoth || o.flip === "x") ? -1 : 1;
		const flipY = o.flip && (flipBoth || o.flip === "y") ? -1 : 1;
		const skewX = o.skew && o.skew.length ? o.skew[0] : isFinite(o.skew) ? o.skew : isFinite(o.skewX) ? o.skewX : 0;
		const skewY = o.skew && o.skew.length ? o.skew[1] : isFinite(o.skew) ? o.skew : isFinite(o.skewY) ? o.skewY : 0;
		const scaleX = o.scale && o.scale.length ? o.scale[0] * flipX : isFinite(o.scale) ? o.scale * flipX : isFinite(o.scaleX) ? o.scaleX * flipX : flipX;
		const scaleY = o.scale && o.scale.length ? o.scale[1] * flipY : isFinite(o.scale) ? o.scale * flipY : isFinite(o.scaleY) ? o.scaleY * flipY : flipY;
		const shear = o.shear || 0;
		const theta = o.rotate || o.theta || 0;
		const origin = new Point(o.origin || o.around || o.ox || o.originX, o.oy || o.originY);
		const ox = origin.x;
		const oy = origin.y;
		const position = new Point(o.position || o.px || o.positionX || NaN, o.py || o.positionY || NaN);
		const px = position.x;
		const py = position.y;
		const translate = new Point(o.translate || o.tx || o.translateX, o.ty || o.translateY);
		const tx = translate.x;
		const ty = translate.y;
		const relative = new Point(o.relative || o.rx || o.relativeX, o.ry || o.relativeY);
		return {
			scaleX,
			scaleY,
			skewX,
			skewY,
			shear,
			theta,
			rx: relative.x,
			ry: relative.y,
			tx,
			ty,
			ox,
			oy,
			px,
			py
		};
	}
	static fromArray(a) {
		return {
			a: a[0],
			b: a[1],
			c: a[2],
			d: a[3],
			e: a[4],
			f: a[5]
		};
	}
	static isMatrixLike(o) {
		return o.a != null || o.b != null || o.c != null || o.d != null || o.e != null || o.f != null;
	}
	static matrixMultiply(l, r, o) {
		const a = l.a * r.a + l.c * r.b;
		const b = l.b * r.a + l.d * r.b;
		const c = l.a * r.c + l.c * r.d;
		const d = l.b * r.c + l.d * r.d;
		const e = l.e + l.a * r.e + l.c * r.f;
		const f = l.f + l.b * r.e + l.d * r.f;
		o.a = a;
		o.b = b;
		o.c = c;
		o.d = d;
		o.e = e;
		o.f = f;
		return o;
	}
	around(cx, cy, matrix) {
		return this.clone().aroundO(cx, cy, matrix);
	}
	aroundO(cx, cy, matrix) {
		const dx = cx || 0;
		const dy = cy || 0;
		return this.translateO(-dx, -dy).lmultiplyO(matrix).translateO(dx, dy);
	}
	clone() {
		return new Matrix(this);
	}
	decompose(cx = 0, cy = 0) {
		const a = this.a;
		const b = this.b;
		const c = this.c;
		const d = this.d;
		const e = this.e;
		const f = this.f;
		const determinant = a * d - b * c;
		const ccw = determinant > 0 ? 1 : -1;
		const sx = ccw * Math.sqrt(a * a + b * b);
		const thetaRad = Math.atan2(ccw * b, ccw * a);
		const theta = 180 / Math.PI * thetaRad;
		const ct = Math.cos(thetaRad);
		const st = Math.sin(thetaRad);
		const lam = (a * c + b * d) / determinant;
		const sy = c * sx / (lam * a - b) || d * sx / (lam * b + a);
		return {
			scaleX: sx,
			scaleY: sy,
			shear: lam,
			rotate: theta,
			translateX: e - cx + cx * ct * sx + cy * (lam * ct * sx - st * sy),
			translateY: f - cy + cx * st * sx + cy * (lam * st * sx + ct * sy),
			originX: cx,
			originY: cy,
			a: this.a,
			b: this.b,
			c: this.c,
			d: this.d,
			e: this.e,
			f: this.f
		};
	}
	equals(other) {
		if (other === this) return true;
		const comp = new Matrix(other);
		return closeEnough(this.a, comp.a) && closeEnough(this.b, comp.b) && closeEnough(this.c, comp.c) && closeEnough(this.d, comp.d) && closeEnough(this.e, comp.e) && closeEnough(this.f, comp.f);
	}
	flip(axis, around) {
		return this.clone().flipO(axis, around);
	}
	flipO(axis, around) {
		return axis === "x" ? this.scaleO(-1, 1, around, 0) : axis === "y" ? this.scaleO(1, -1, 0, around) : this.scaleO(-1, -1, axis, around || axis);
	}
	init(source) {
		const base = Matrix.fromArray([
			1,
			0,
			0,
			1,
			0,
			0
		]);
		source = source instanceof Element ? source.matrixify() : typeof source === "string" ? Matrix.fromArray(source.split(delimiter).map(parseFloat)) : Array.isArray(source) ? Matrix.fromArray(source) : typeof source === "object" && Matrix.isMatrixLike(source) ? source : typeof source === "object" ? new Matrix().transform(source) : arguments.length === 6 ? Matrix.fromArray([].slice.call(arguments)) : base;
		this.a = source.a != null ? source.a : base.a;
		this.b = source.b != null ? source.b : base.b;
		this.c = source.c != null ? source.c : base.c;
		this.d = source.d != null ? source.d : base.d;
		this.e = source.e != null ? source.e : base.e;
		this.f = source.f != null ? source.f : base.f;
		return this;
	}
	inverse() {
		return this.clone().inverseO();
	}
	inverseO() {
		const a = this.a;
		const b = this.b;
		const c = this.c;
		const d = this.d;
		const e = this.e;
		const f = this.f;
		const det = a * d - b * c;
		if (!det) throw new Error("Cannot invert " + this);
		const na = d / det;
		const nb = -b / det;
		const nc = -c / det;
		const nd = a / det;
		const ne = -(na * e + nc * f);
		const nf = -(nb * e + nd * f);
		this.a = na;
		this.b = nb;
		this.c = nc;
		this.d = nd;
		this.e = ne;
		this.f = nf;
		return this;
	}
	lmultiply(matrix) {
		return this.clone().lmultiplyO(matrix);
	}
	lmultiplyO(matrix) {
		const r = this;
		const l = matrix instanceof Matrix ? matrix : new Matrix(matrix);
		return Matrix.matrixMultiply(l, r, this);
	}
	multiply(matrix) {
		return this.clone().multiplyO(matrix);
	}
	multiplyO(matrix) {
		const l = this;
		const r = matrix instanceof Matrix ? matrix : new Matrix(matrix);
		return Matrix.matrixMultiply(l, r, this);
	}
	rotate(r, cx, cy) {
		return this.clone().rotateO(r, cx, cy);
	}
	rotateO(r, cx = 0, cy = 0) {
		r = radians(r);
		const cos = Math.cos(r);
		const sin = Math.sin(r);
		const { a, b, c, d, e, f } = this;
		this.a = a * cos - b * sin;
		this.b = b * cos + a * sin;
		this.c = c * cos - d * sin;
		this.d = d * cos + c * sin;
		this.e = e * cos - f * sin + cy * sin - cx * cos + cx;
		this.f = f * cos + e * sin - cx * sin - cy * cos + cy;
		return this;
	}
	scale() {
		return this.clone().scaleO(...arguments);
	}
	scaleO(x, y = x, cx = 0, cy = 0) {
		if (arguments.length === 3) {
			cy = cx;
			cx = y;
			y = x;
		}
		const { a, b, c, d, e, f } = this;
		this.a = a * x;
		this.b = b * y;
		this.c = c * x;
		this.d = d * y;
		this.e = e * x - cx * x + cx;
		this.f = f * y - cy * y + cy;
		return this;
	}
	shear(a, cx, cy) {
		return this.clone().shearO(a, cx, cy);
	}
	shearO(lx, cx = 0, cy = 0) {
		const { a, b, c, d, e, f } = this;
		this.a = a + b * lx;
		this.c = c + d * lx;
		this.e = e + f * lx - cy * lx;
		return this;
	}
	skew() {
		return this.clone().skewO(...arguments);
	}
	skewO(x, y = x, cx = 0, cy = 0) {
		if (arguments.length === 3) {
			cy = cx;
			cx = y;
			y = x;
		}
		x = radians(x);
		y = radians(y);
		const lx = Math.tan(x);
		const ly = Math.tan(y);
		const { a, b, c, d, e, f } = this;
		this.a = a + b * lx;
		this.b = b + a * ly;
		this.c = c + d * lx;
		this.d = d + c * ly;
		this.e = e + f * lx - cy * lx;
		this.f = f + e * ly - cx * ly;
		return this;
	}
	skewX(x, cx, cy) {
		return this.skew(x, 0, cx, cy);
	}
	skewY(y, cx, cy) {
		return this.skew(0, y, cx, cy);
	}
	toArray() {
		return [
			this.a,
			this.b,
			this.c,
			this.d,
			this.e,
			this.f
		];
	}
	toString() {
		return "matrix(" + this.a + "," + this.b + "," + this.c + "," + this.d + "," + this.e + "," + this.f + ")";
	}
	transform(o) {
		if (Matrix.isMatrixLike(o)) return new Matrix(o).multiplyO(this);
		const t = Matrix.formatTransforms(o);
		const current = this;
		const { x: ox, y: oy } = new Point(t.ox, t.oy).transform(current);
		const transformer = new Matrix().translateO(t.rx, t.ry).lmultiplyO(current).translateO(-ox, -oy).scaleO(t.scaleX, t.scaleY).skewO(t.skewX, t.skewY).shearO(t.shear).rotateO(t.theta).translateO(ox, oy);
		if (isFinite(t.px) || isFinite(t.py)) {
			const origin = new Point(ox, oy).transform(transformer);
			const dx = isFinite(t.px) ? t.px - origin.x : 0;
			const dy = isFinite(t.py) ? t.py - origin.y : 0;
			transformer.translateO(dx, dy);
		}
		transformer.translateO(t.tx, t.ty);
		return transformer;
	}
	translate(x, y) {
		return this.clone().translateO(x, y);
	}
	translateO(x, y) {
		this.e += x || 0;
		this.f += y || 0;
		return this;
	}
	valueOf() {
		return {
			a: this.a,
			b: this.b,
			c: this.c,
			d: this.d,
			e: this.e,
			f: this.f
		};
	}
};
function ctm() {
	return new Matrix(this.node.getCTM());
}
function screenCTM() {
	try {
		if (typeof this.isRoot === "function" && !this.isRoot()) {
			const rect = this.rect(1, 1);
			const m = rect.node.getScreenCTM();
			rect.remove();
			return new Matrix(m);
		}
		return new Matrix(this.node.getScreenCTM());
	} catch (e) {
		console.warn(`Cannot get CTM from SVG node ${this.node.nodeName}. Is the element rendered?`);
		return new Matrix();
	}
}
register(Matrix, "Matrix");
//#endregion
export { ctm, Matrix as default, screenCTM };
