import { registerMethods } from "../utils/methods.js";
import { xlink } from "../modules/core/namespaces.js";
import { nodeOrNew, register, wrapWithAttrCheck } from "../utils/adopter.js";
import baseFind from "../modules/core/selector.js";
import PathArray from "../types/PathArray.js";
import Path from "./Path.js";
import Text from "./Text.js";
//#region node_modules/@svgdotjs/svg.js/src/elements/TextPath.js
var TextPath = class extends Text {
	constructor(node, attrs = node) {
		super(nodeOrNew("textPath", node), attrs);
	}
	array() {
		const track = this.track();
		return track ? track.array() : null;
	}
	plot(d) {
		const track = this.track();
		let pathArray = null;
		if (track) pathArray = track.plot(d);
		return d == null ? pathArray : this;
	}
	track() {
		return this.reference("href");
	}
};
registerMethods({
	Container: { textPath: wrapWithAttrCheck(function(text, path) {
		if (!(text instanceof Text)) text = this.text(text);
		return text.path(path);
	}) },
	Text: {
		path: wrapWithAttrCheck(function(track, importNodes = true) {
			const textPath = new TextPath();
			if (!(track instanceof Path)) track = this.defs().path(track);
			textPath.attr("href", "#" + track, xlink);
			let node;
			if (importNodes) while (node = this.node.firstChild) textPath.node.appendChild(node);
			return this.put(textPath);
		}),
		textPath() {
			return this.findOne("textPath");
		}
	},
	Path: {
		text: wrapWithAttrCheck(function(text) {
			if (!(text instanceof Text)) text = new Text().addTo(this.parent()).text(text);
			return text.path(this);
		}),
		targets() {
			return baseFind("svg textPath").filter((node) => {
				return (node.attr("href") || "").includes(this.id());
			});
		}
	}
});
TextPath.prototype.MorphArray = PathArray;
register(TextPath, "TextPath");
//#endregion
export { TextPath as default };
