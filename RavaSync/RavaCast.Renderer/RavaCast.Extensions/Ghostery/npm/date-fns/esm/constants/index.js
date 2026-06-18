//#region node_modules/date-fns/esm/constants/index.js
/**
* Days in 1 year
* One years equals 365.2425 days according to the formula:
*
* > Leap year occures every 4 years, except for years that are divisable by 100 and not divisable by 400.
* > 1 mean year = (365+1/4-1/100+1/400) days = 365.2425 days
*
* @name daysInYear
* @constant
* @type {number}
* @default
*/
var daysInYear = 365.2425;
/**
* Maximum allowed time.
*
* @name maxTime
* @constant
* @type {number}
* @default
*/
var maxTime = Math.pow(10, 8) * 24 * 60 * 60 * 1e3;
/**
* Milliseconds in 1 minute
*
* @name millisecondsInMinute
* @constant
* @type {number}
* @default
*/
var millisecondsInMinute = 6e4;
/**
* Milliseconds in 1 hour
*
* @name millisecondsInHour
* @constant
* @type {number}
* @default
*/
var millisecondsInHour = 36e5;
-maxTime;
/**
* Seconds in 1 day
*
* @name secondsInDay
* @constant
* @type {number}
* @default
*/
var secondsInDay = 3600 * 24;
secondsInDay * 7;
secondsInDay * daysInYear / 12 * 3;
//#endregion
export { millisecondsInHour, millisecondsInMinute };
