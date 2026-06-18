import { registerMethods } from "../../utils/methods.js";
import { getOrigin, isDescriptive } from "../../utils/utils.js";
import { delimiter, transforms } from "../core/regex.js";
import Matrix from "../../types/Matrix.js";
//#region node_modules/@svgdotjs/svg.js/src/modules/optional/transform.js
function untransform() {
	return this.attr("transform", null);
}
function matrixify() {
	return (this.attr("transform") || "").split(transforms).slice(0, -1).map(function(str) {
		const kv = str.trim().split("(");
		return [kv[0], kv[1].split(delimiter).map(function(str) {
			return parseFloat(str);
		})];
	}).reverse().reduce(function(matrix, transform) {
		if (transform[0] === "matrix") return matrix.lmultiply(Matrix.fromArray(transform[1]));
		return matrix[transform[0]].apply(matrix, transform[1]);
	}, new Matrix());
}
function toParent(parent, i) {
	if (this === parent) return this;
	if (isDescriptive(this.node)) return this.addTo(parent, i);
	const ctm = this.screenCTM();
	const pCtm = parent.screenCTM().inverse();
	this.addTo(parent, i).untransform().transform(pCtm.multiply(ctm));
	return this;
}
function toRoot(i) {
	return this.toParent(this.root(), i);
}
function transform(o, relative) {
	if (o == null || typeof o === "string") {
		const decomposed = new Matrix(this).decompose();
		return o == null ? decomposed : decomposed[o];
	}
	if (!Matrix.isMatrixLike(o)) o = {
		...o,
		origin: getOrigin(o, this)
	};
	const result = new Matrix(relative === true ? this : relative || false).transform(o);
	return this.attr("transform", result);
}
registerMethods("Element", {
	untransform,
	matrixify,
	toParent,
	toRoot,
	transform
});
//#endregion
