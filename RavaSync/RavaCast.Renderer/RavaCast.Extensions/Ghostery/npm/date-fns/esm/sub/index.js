import { _typeof } from "../../../@babel/runtime/helpers/esm/typeof.js";
import toInteger from "../_lib/toInteger/index.js";
import requiredArgs from "../_lib/requiredArgs/index.js";
import subDays from "../subDays/index.js";
import subMonths from "../subMonths/index.js";
//#region node_modules/date-fns/esm/sub/index.js
/**
* @name sub
* @category Common Helpers
* @summary Subtract the specified years, months, weeks, days, hours, minutes and seconds from the given date.
*
* @description
* Subtract the specified years, months, weeks, days, hours, minutes and seconds from the given date.
*
* @param {Date|Number} date - the date to be changed
* @param {Duration} duration - the object with years, months, weeks, days, hours, minutes and seconds to be subtracted
*
* | Key     | Description                        |
* |---------|------------------------------------|
* | years   | Amount of years to be subtracted   |
* | months  | Amount of months to be subtracted  |
* | weeks   | Amount of weeks to be subtracted   |
* | days    | Amount of days to be subtracted    |
* | hours   | Amount of hours to be subtracted   |
* | minutes | Amount of minutes to be subtracted |
* | seconds | Amount of seconds to be subtracted |
*
* All values default to 0
*
* @returns {Date} the new date with the seconds subtracted
* @throws {TypeError} 2 arguments required
*
* @example
* // Subtract the following duration from 15 June 2017 15:29:20
* const result = sub(new Date(2017, 5, 15, 15, 29, 20), {
*   years: 2,
*   months: 9,
*   weeks: 1,
*   days: 7,
*   hours: 5,
*   minutes: 9,
*   seconds: 30
* })
* //=> Mon Sep 1 2014 10:19:50
*/
function sub(date, duration) {
	requiredArgs(2, arguments);
	if (!duration || _typeof(duration) !== "object") return /* @__PURE__ */ new Date(NaN);
	var years = duration.years ? toInteger(duration.years) : 0;
	var months = duration.months ? toInteger(duration.months) : 0;
	var weeks = duration.weeks ? toInteger(duration.weeks) : 0;
	var days = duration.days ? toInteger(duration.days) : 0;
	var hours = duration.hours ? toInteger(duration.hours) : 0;
	var minutes = duration.minutes ? toInteger(duration.minutes) : 0;
	var seconds = duration.seconds ? toInteger(duration.seconds) : 0;
	var dateWithoutDays = subDays(subMonths(date, months + years * 12), days + weeks * 7);
	var mstoSub = (seconds + (minutes + hours * 60) * 60) * 1e3;
	return new Date(dateWithoutDays.getTime() - mstoSub);
}
//#endregion
export { sub as default };
