import { MIME } from "../shared/symbols.js";
import { Document } from "../interface/document.js";
//#region node_modules/linkedom/esm/svg/document.js
/**
* @implements globalThis.Document
*/
var SVGDocument = class extends Document {
	constructor() {
		super("image/svg+xml");
	}
	toString() {
		return this[MIME].docType + super.toString();
	}
};
//#endregion
export { SVGDocument };
