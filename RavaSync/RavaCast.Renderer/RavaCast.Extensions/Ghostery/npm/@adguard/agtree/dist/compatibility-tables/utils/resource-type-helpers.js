import { isNull } from "../../utils/type-guards.js";
import { modifiersCompatibilityTable } from "../modifiers.js";
import { ResourceType } from "../schemas/resource-type.js";
//#region node_modules/@adguard/agtree/dist/compatibility-tables/utils/resource-type-helpers.js
/**
* Map of resource types to their corresponding adblock modifier names.
*
* @note Record type is used to ensure that all resource types are present in the map.
*/
var RESOURCE_TYPE_MODIFIER_MAP = Object.freeze({
	[ResourceType.MainFrame]: "document",
	[ResourceType.SubFrame]: "subdocument",
	[ResourceType.Stylesheet]: "stylesheet",
	[ResourceType.Script]: "script",
	[ResourceType.Image]: "image",
	[ResourceType.Font]: "font",
	[ResourceType.Object]: "object",
	[ResourceType.XmlHttpRequest]: "xmlhttprequest",
	[ResourceType.Ping]: "ping",
	[ResourceType.Media]: "media",
	[ResourceType.WebSocket]: "websocket",
	[ResourceType.Other]: "other"
});
/**
* Gets the adblock modifier name for the given resource type.
*
* @param resourceType Resource type to get the modifier name for.
* @param platform Platform to get the modifier for (can be specific, generic, or combined platforms).
*
* @returns A string containing the adblock modifier name for the given resource type
* or `null` if the modifier could not be found.
*/
var getResourceTypeModifier = (resourceType, platform) => {
	const modifierName = RESOURCE_TYPE_MODIFIER_MAP[resourceType];
	if (!modifierName) return null;
	const modifierData = modifiersCompatibilityTable.getFirst(modifierName, platform);
	if (isNull(modifierData)) return null;
	return modifierData.name;
};
/**
* Checks if the given resource type is valid.
*
* @param resourceType Resource type to check.
*
* @returns `true` if the resource type is valid, `false` otherwise.
*/
var isValidResourceType = (resourceType) => {
	return Object.values(ResourceType).includes(resourceType);
};
//#endregion
export { getResourceTypeModifier, isValidResourceType };
