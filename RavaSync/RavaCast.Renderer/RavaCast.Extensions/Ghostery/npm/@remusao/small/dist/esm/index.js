import "./types.js";
import resource from "./flv.js";
import resource$1 from "./gif.js";
import resource$2 from "./html.js";
import resource$3 from "./ico.js";
import resource$4 from "./jpeg.js";
import resource$5 from "./javascript.js";
import resource$6 from "./json.js";
import resource$7 from "./mp3.js";
import resource$8 from "./mp4.js";
import resource$9 from "./pdf.js";
import resource$10 from "./png.js";
import resource$11 from "./svg.js";
import resource$12 from "./txt.js";
import resource$13 from "./wav.js";
import resource$14 from "./webm.js";
import resource$15 from "./webp.js";
import resource$16 from "./wmv.js";
//#region node_modules/@remusao/small/dist/esm/index.js
var MIME_TO_RESOURCE = (() => {
	const resources = {};
	for (const fake of [
		resource,
		resource$1,
		resource$2,
		resource$3,
		resource$4,
		resource$5,
		resource$6,
		resource$7,
		resource$8,
		resource$9,
		resource$10,
		resource$11,
		resource$12,
		resource$13,
		resource$14,
		resource$15,
		resource$16
	]) for (const alias of fake.aliases) resources[alias] = fake;
	return resources;
})();
function getFallbackTextResource() {
	return resource$12;
}
function getResourceForMime(mime) {
	return MIME_TO_RESOURCE[mime] || getFallbackTextResource();
}
//#endregion
export { getResourceForMime };
