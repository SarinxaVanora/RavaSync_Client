import { registerMethods } from "../utils/methods.js";
import { isDescriptive, writeDataToDom } from "../utils/utils.js";
import { globals } from "../utils/window.js";
import { adopt, extend, nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import SVGNumber from "../types/SVGNumber.js";
import Shape from "./Shape.js";
import { textable_exports } from "../modules/core/textable.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/Text.js
var Text = class extends Shape {
	constructor(node, attrs = node) {
		super(nodeOrNew("text", node), attrs);
		this.dom.leading = this.dom.leading ?? new SVGNumber(1.3);
		this._rebuild = true;
		this._build = false;
	}
	leading(value) {
		if (value == null) return this.dom.leading;
		this.dom.leading = new SVGNumber(value);
		return this.rebuild();
	}
	rebuild(rebuild) {
		if (typeof rebuild === "boolean") this._rebuild = rebuild;
		if (this._rebuild) {
			const self = this;
			let blankLineOffset = 0;
			const leading = this.dom.leading;
			this.each(function(i) {
				if (isDescriptive(this.node)) return;
				const dy = leading * new SVGNumber(globals.window.getComputedStyle(this.node).getPropertyValue("font-size"));
				if (this.dom.newLined) {
					this.attr("x", self.attr("x"));
					if (this.text() === "\n") blankLineOffset += dy;
					else {
						this.attr("dy", i ? dy + blankLineOffset : 0);
						blankLineOffset = 0;
					}
				}
			});
			this.fire("rebuild");
		}
		return this;
	}
	setData(o) {
		this.dom = o;
		this.dom.leading = new SVGNumber(o.leading || 1.3);
		return this;
	}
	writeDataToDom() {
		writeDataToDom(this, this.dom, { leading: 1.3 });
		return this;
	}
	text(text) {
		if (text === void 0) {
			const children = this.node.childNodes;
			let firstLine = 0;
			text = "";
			for (let i = 0, len = children.length; i < len; ++i) {
				if (children[i].nodeName === "textPath" || isDescriptive(children[i])) {
					if (i === 0) firstLine = i + 1;
					continue;
				}
				if (i !== firstLine && children[i].nodeType !== 3 && adopt(children[i]).dom.newLined === true) text += "\n";
				text += children[i].textContent;
			}
			return text;
		}
		this.clear().build(true);
		if (typeof text === "function") text.call(this, this);
		else {
			text = (text + "").split("\n");
			for (let j = 0, jl = text.length; j < jl; j++) this.newLine(text[j]);
		}
		return this.build(false).rebuild();
	}
};
extend(Text, textable_exports);
registerMethods({ Container: {
	text: wrapWithAttrCheck(function(text = "") {
		return this.put(new Text()).text(text);
	}),
	plain: wrapWithAttrCheck(function(text = "") {
		return this.put(new Text()).plain(text);
	})
} });
register(Text, "Text");
//#endregion
export { Text as default };
