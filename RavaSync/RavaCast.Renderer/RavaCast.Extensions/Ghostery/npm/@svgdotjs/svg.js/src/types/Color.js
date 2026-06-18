import { hex, isHex, isRgb, rgb, whitespace } from "../modules/core/regex.js";
//#region node_modules/@svgdotjs/svg.js/src/types/Color.js
function sixDigitHex(hex) {
	return hex.length === 4 ? [
		"#",
		hex.substring(1, 2),
		hex.substring(1, 2),
		hex.substring(2, 3),
		hex.substring(2, 3),
		hex.substring(3, 4),
		hex.substring(3, 4)
	].join("") : hex;
}
function componentHex(component) {
	const hex = Math.max(0, Math.min(255, Math.round(component))).toString(16);
	return hex.length === 1 ? "0" + hex : hex;
}
function is(object, space) {
	for (let i = space.length; i--;) if (object[space[i]] == null) return false;
	return true;
}
function getParameters(a, b) {
	const params = is(a, "rgb") ? {
		_a: a.r,
		_b: a.g,
		_c: a.b,
		_d: 0,
		space: "rgb"
	} : is(a, "xyz") ? {
		_a: a.x,
		_b: a.y,
		_c: a.z,
		_d: 0,
		space: "xyz"
	} : is(a, "hsl") ? {
		_a: a.h,
		_b: a.s,
		_c: a.l,
		_d: 0,
		space: "hsl"
	} : is(a, "lab") ? {
		_a: a.l,
		_b: a.a,
		_c: a.b,
		_d: 0,
		space: "lab"
	} : is(a, "lch") ? {
		_a: a.l,
		_b: a.c,
		_c: a.h,
		_d: 0,
		space: "lch"
	} : is(a, "cmyk") ? {
		_a: a.c,
		_b: a.m,
		_c: a.y,
		_d: a.k,
		space: "cmyk"
	} : {
		_a: 0,
		_b: 0,
		_c: 0,
		space: "rgb"
	};
	params.space = b || params.space;
	return params;
}
function cieSpace(space) {
	if (space === "lab" || space === "xyz" || space === "lch") return true;
	else return false;
}
function hueToRgb(p, q, t) {
	if (t < 0) t += 1;
	if (t > 1) t -= 1;
	if (t < 1 / 6) return p + (q - p) * 6 * t;
	if (t < 1 / 2) return q;
	if (t < 2 / 3) return p + (q - p) * (2 / 3 - t) * 6;
	return p;
}
var Color = class Color {
	constructor(...inputs) {
		this.init(...inputs);
	}
	static isColor(color) {
		return color && (color instanceof Color || this.isRgb(color) || this.test(color));
	}
	static isRgb(color) {
		return color && typeof color.r === "number" && typeof color.g === "number" && typeof color.b === "number";
	}
	static random(mode = "vibrant", t) {
		const { random, round, sin, PI: pi } = Math;
		if (mode === "vibrant") return new Color(24 * random() + 57, 38 * random() + 45, 360 * random(), "lch");
		else if (mode === "sine") {
			t = t == null ? random() : t;
			return new Color(round(80 * sin(2 * pi * t / .5 + .01) + 150), round(50 * sin(2 * pi * t / .5 + 4.6) + 200), round(100 * sin(2 * pi * t / .5 + 2.3) + 150));
		} else if (mode === "pastel") return new Color(8 * random() + 86, 17 * random() + 9, 360 * random(), "lch");
		else if (mode === "dark") return new Color(10 + 10 * random(), 50 * random() + 86, 360 * random(), "lch");
		else if (mode === "rgb") return new Color(255 * random(), 255 * random(), 255 * random());
		else if (mode === "lab") return new Color(100 * random(), 256 * random() - 128, 256 * random() - 128, "lab");
		else if (mode === "grey") {
			const grey = 255 * random();
			return new Color(grey, grey, grey);
		} else throw new Error("Unsupported random color mode");
	}
	static test(color) {
		return typeof color === "string" && (isHex.test(color) || isRgb.test(color));
	}
	cmyk() {
		const { _a, _b, _c } = this.rgb();
		const [r, g, b] = [
			_a,
			_b,
			_c
		].map((v) => v / 255);
		const k = Math.min(1 - r, 1 - g, 1 - b);
		if (k === 1) return new Color(0, 0, 0, 1, "cmyk");
		return new Color((1 - r - k) / (1 - k), (1 - g - k) / (1 - k), (1 - b - k) / (1 - k), k, "cmyk");
	}
	hsl() {
		const { _a, _b, _c } = this.rgb();
		const [r, g, b] = [
			_a,
			_b,
			_c
		].map((v) => v / 255);
		const max = Math.max(r, g, b);
		const min = Math.min(r, g, b);
		const l = (max + min) / 2;
		const isGrey = max === min;
		const delta = max - min;
		const s = isGrey ? 0 : l > .5 ? delta / (2 - max - min) : delta / (max + min);
		return new Color(360 * (isGrey ? 0 : max === r ? ((g - b) / delta + (g < b ? 6 : 0)) / 6 : max === g ? ((b - r) / delta + 2) / 6 : max === b ? ((r - g) / delta + 4) / 6 : 0), 100 * s, 100 * l, "hsl");
	}
	init(a = 0, b = 0, c = 0, d = 0, space = "rgb") {
		a = !a ? 0 : a;
		if (this.space) for (const component in this.space) delete this[this.space[component]];
		if (typeof a === "number") {
			space = typeof d === "string" ? d : space;
			d = typeof d === "string" ? 0 : d;
			Object.assign(this, {
				_a: a,
				_b: b,
				_c: c,
				_d: d,
				space
			});
		} else if (a instanceof Array) {
			this.space = b || (typeof a[3] === "string" ? a[3] : a[4]) || "rgb";
			Object.assign(this, {
				_a: a[0],
				_b: a[1],
				_c: a[2],
				_d: a[3] || 0
			});
		} else if (a instanceof Object) {
			const values = getParameters(a, b);
			Object.assign(this, values);
		} else if (typeof a === "string") if (isRgb.test(a)) {
			const noWhitespace = a.replace(whitespace, "");
			const [_a, _b, _c] = rgb.exec(noWhitespace).slice(1, 4).map((v) => parseInt(v));
			Object.assign(this, {
				_a,
				_b,
				_c,
				_d: 0,
				space: "rgb"
			});
		} else if (isHex.test(a)) {
			const hexParse = (v) => parseInt(v, 16);
			const [, _a, _b, _c] = hex.exec(sixDigitHex(a)).map(hexParse);
			Object.assign(this, {
				_a,
				_b,
				_c,
				_d: 0,
				space: "rgb"
			});
		} else throw Error("Unsupported string format, can't construct Color");
		const { _a, _b, _c, _d } = this;
		const components = this.space === "rgb" ? {
			r: _a,
			g: _b,
			b: _c
		} : this.space === "xyz" ? {
			x: _a,
			y: _b,
			z: _c
		} : this.space === "hsl" ? {
			h: _a,
			s: _b,
			l: _c
		} : this.space === "lab" ? {
			l: _a,
			a: _b,
			b: _c
		} : this.space === "lch" ? {
			l: _a,
			c: _b,
			h: _c
		} : this.space === "cmyk" ? {
			c: _a,
			m: _b,
			y: _c,
			k: _d
		} : {};
		Object.assign(this, components);
	}
	lab() {
		const { x, y, z } = this.xyz();
		return new Color(116 * y - 16, 500 * (x - y), 200 * (y - z), "lab");
	}
	lch() {
		const { l, a, b } = this.lab();
		const c = Math.sqrt(a ** 2 + b ** 2);
		let h = 180 * Math.atan2(b, a) / Math.PI;
		if (h < 0) {
			h *= -1;
			h = 360 - h;
		}
		return new Color(l, c, h, "lch");
	}
	rgb() {
		if (this.space === "rgb") return this;
		else if (cieSpace(this.space)) {
			let { x, y, z } = this;
			if (this.space === "lab" || this.space === "lch") {
				let { l, a, b } = this;
				if (this.space === "lch") {
					const { c, h } = this;
					const dToR = Math.PI / 180;
					a = c * Math.cos(dToR * h);
					b = c * Math.sin(dToR * h);
				}
				const yL = (l + 16) / 116;
				const xL = a / 500 + yL;
				const zL = yL - b / 200;
				const ct = 16 / 116;
				const mx = .008856;
				const nm = 7.787;
				x = .95047 * (xL ** 3 > mx ? xL ** 3 : (xL - ct) / nm);
				y = 1 * (yL ** 3 > mx ? yL ** 3 : (yL - ct) / nm);
				z = 1.08883 * (zL ** 3 > mx ? zL ** 3 : (zL - ct) / nm);
			}
			const rU = x * 3.2406 + y * -1.5372 + z * -.4986;
			const gU = x * -.9689 + y * 1.8758 + z * .0415;
			const bU = x * .0557 + y * -.204 + z * 1.057;
			const pow = Math.pow;
			const bd = .0031308;
			const r = rU > bd ? 1.055 * pow(rU, 1 / 2.4) - .055 : 12.92 * rU;
			const g = gU > bd ? 1.055 * pow(gU, 1 / 2.4) - .055 : 12.92 * gU;
			const b = bU > bd ? 1.055 * pow(bU, 1 / 2.4) - .055 : 12.92 * bU;
			return new Color(255 * r, 255 * g, 255 * b);
		} else if (this.space === "hsl") {
			let { h, s, l } = this;
			h /= 360;
			s /= 100;
			l /= 100;
			if (s === 0) {
				l *= 255;
				return new Color(l, l, l);
			}
			const q = l < .5 ? l * (1 + s) : l + s - l * s;
			const p = 2 * l - q;
			return new Color(255 * hueToRgb(p, q, h + 1 / 3), 255 * hueToRgb(p, q, h), 255 * hueToRgb(p, q, h - 1 / 3));
		} else if (this.space === "cmyk") {
			const { c, m, y, k } = this;
			return new Color(255 * (1 - Math.min(1, c * (1 - k) + k)), 255 * (1 - Math.min(1, m * (1 - k) + k)), 255 * (1 - Math.min(1, y * (1 - k) + k)));
		} else return this;
	}
	toArray() {
		const { _a, _b, _c, _d, space } = this;
		return [
			_a,
			_b,
			_c,
			_d,
			space
		];
	}
	toHex() {
		const [r, g, b] = this._clamped().map(componentHex);
		return `#${r}${g}${b}`;
	}
	toRgb() {
		const [rV, gV, bV] = this._clamped();
		return `rgb(${rV},${gV},${bV})`;
	}
	toString() {
		return this.toHex();
	}
	xyz() {
		const { _a: r255, _b: g255, _c: b255 } = this.rgb();
		const [r, g, b] = [
			r255,
			g255,
			b255
		].map((v) => v / 255);
		const rL = r > .04045 ? Math.pow((r + .055) / 1.055, 2.4) : r / 12.92;
		const gL = g > .04045 ? Math.pow((g + .055) / 1.055, 2.4) : g / 12.92;
		const bL = b > .04045 ? Math.pow((b + .055) / 1.055, 2.4) : b / 12.92;
		const xU = (rL * .4124 + gL * .3576 + bL * .1805) / .95047;
		const yU = (rL * .2126 + gL * .7152 + bL * .0722) / 1;
		const zU = (rL * .0193 + gL * .1192 + bL * .9505) / 1.08883;
		return new Color(xU > .008856 ? Math.pow(xU, 1 / 3) : 7.787 * xU + 16 / 116, yU > .008856 ? Math.pow(yU, 1 / 3) : 7.787 * yU + 16 / 116, zU > .008856 ? Math.pow(zU, 1 / 3) : 7.787 * zU + 16 / 116, "xyz");
	}
	_clamped() {
		const { _a, _b, _c } = this.rgb();
		const { max, min, round } = Math;
		const format = (v) => max(0, min(round(v), 255));
		return [
			_a,
			_b,
			_c
		].map(format);
	}
};
//#endregion
export { Color as default };
