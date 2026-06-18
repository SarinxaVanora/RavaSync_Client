import { z } from "../../../../../zod/lib/index.js";
//#region node_modules/@adguard/agtree/dist/compatibility-tables/schemas/resource-type.js
/**
* @file Resource type schema.
*/
/**
* Resource type.
*
* @see {@link https://developer.chrome.com/docs/extensions/reference/declarativeNetRequest/#type-ResourceType}
*/
var ResourceType = {
	MainFrame: "main_frame",
	SubFrame: "sub_frame",
	Stylesheet: "stylesheet",
	Script: "script",
	Image: "image",
	Font: "font",
	Object: "object",
	XmlHttpRequest: "xmlhttprequest",
	Ping: "ping",
	Media: "media",
	WebSocket: "websocket",
	Other: "other"
};
z.nativeEnum(ResourceType);
//#endregion
export { ResourceType };
