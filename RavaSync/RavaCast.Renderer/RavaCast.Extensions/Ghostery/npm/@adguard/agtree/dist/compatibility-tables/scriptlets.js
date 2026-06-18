import { CompatibilityTableBase } from "./base.js";
import { scriptletsCompatibilityTableData } from "./compatibility-table-data.js";
import { deepFreeze } from "../utils/deep-freeze.js";
//#region node_modules/@adguard/agtree/dist/compatibility-tables/scriptlets.js
/**
* @file Compatibility tables for scriptlets.
*/
/**
* Compatibility table for scriptlets.
*/
var ScriptletsCompatibilityTable = class extends CompatibilityTableBase {};
/**
* Deep freeze the compatibility table data to avoid accidental modifications.
*/
deepFreeze(scriptletsCompatibilityTableData);
/**
* Compatibility table instance for scriptlets.
*/
var scriptletsCompatibilityTable = new ScriptletsCompatibilityTable(scriptletsCompatibilityTableData);
//#endregion
export { scriptletsCompatibilityTable };
