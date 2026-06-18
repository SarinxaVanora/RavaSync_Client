import { VALUE } from "../shared/symbols.js";
import { CharacterData } from "./character-data.js";
//#region node_modules/linkedom/esm/interface/comment.js
/**
* @implements globalThis.Comment
*/
var Comment = class Comment extends CharacterData {
	constructor(ownerDocument, data = "") {
		super(ownerDocument, "#comment", 8, data);
	}
	cloneNode() {
		const { ownerDocument, [VALUE]: data } = this;
		return new Comment(ownerDocument, data);
	}
	toString() {
		return `<!--${this[VALUE]}-->`;
	}
};
//#endregion
export { Comment };
