import { PREFIX } from "./types.js";
//#region node_modules/@remusao/small/dist/esm/mp3.js
var CONTENT_TYPE = "audio/mpeg";
var resource = {
	name: `${PREFIX}.mp3`,
	contentType: `${CONTENT_TYPE};base64`,
	aliases: [
		CONTENT_TYPE,
		".mp3",
		"mp3",
		"noop-0.1s.mp3",
		"noopmp3-0.1s"
	],
	body: "/+MYxAAAAANIAAAAAExBTUUzLjk4LjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
};
//#endregion
export { resource as default };
