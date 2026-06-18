import toInteger from "../_lib/toInteger/index.js";
import requiredArgs from "../_lib/requiredArgs/index.js";
import toDate from "../toDate/index.js";
//#region node_modules/date-fns/esm/addMonths/index.js
/**
* @name addMonths
* @category Month Helpers
* @summary Add the specified number of months to the given date.
*
* @description
* Add the specified number of months to the given date.
*
* @param {Date|Number} date - the date to be changed
* @param {Number} amount - the amount of months to be added. Positive decimals will be rounded using `Math.floor`, decimals less than zero will be rounded using `Math.ceil`.
* @returns {Date} the new date with the months added
* @throws {TypeError} 2 arguments required
*
* @example
* // Add 5 months to 1 September 2014:
* const result = addMonths(new Date(2014, 8, 1), 5)
* //=> Sun Feb 01 2015 00:00:00
*/
function addMonths(dirtyDate, dirtyAmount) {
	requiredArgs(2, arguments);
	var date = toDate(dirtyDate);
	var amount = toInteger(dirtyAmount);
	if (isNaN(amount)) return /* @__PURE__ */ new Date(NaN);
	if (!amount) return date;
	var dayOfMonth = date.getDate();
	var endOfDesiredMonth = new Date(date.getTime());
	endOfDesiredMonth.setMonth(date.getMonth() + amount + 1, 0);
	if (dayOfMonth >= endOfDesiredMonth.getDate()) return endOfDesiredMonth;
	else {
		date.setFullYear(endOfDesiredMonth.getFullYear(), endOfDesiredMonth.getMonth(), dayOfMonth);
		return date;
	}
}
//#endregion
export { addMonths as default };
