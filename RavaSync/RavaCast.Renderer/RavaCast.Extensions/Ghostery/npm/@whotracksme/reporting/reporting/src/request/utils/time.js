//#region node_modules/@whotracksme/reporting/reporting/src/request/utils/time.js
/** Get datetime string of the current hour in the format YYYYMMDDHH
*/
function getTime() {
	const date = /* @__PURE__ */ new Date();
	let _ts;
	let h = null;
	{
		let d = null;
		let m = null;
		let y = null;
		d = (date.getDate() < 10 ? "0" : "") + date.getDate();
		m = (date.getMonth() < 9 ? "0" : "") + parseInt(date.getMonth() + 1, 10);
		h = (date.getUTCHours() < 10 ? "0" : "") + date.getUTCHours();
		y = date.getFullYear();
		_ts = y + "" + m + d + h;
	}
	return _ts;
}
function newUTCDate() {
	const dayHour = getTime();
	return new Date(Date.UTC(dayHour.substring(0, 4), parseInt(dayHour.substring(4, 6), 10) - 1, dayHour.substring(6, 8), dayHour.substring(8, 10)));
}
function dateString(date) {
	const yyyy = date.getFullYear().toString();
	const mm = (date.getMonth() + 1).toString();
	const dd = date.getDate().toString();
	return yyyy + (mm[1] ? mm : `0${mm[0]}`) + (dd[1] ? dd : `0${dd[0]}`);
}
function getCurrentDay() {
	if (!getCurrentDay._currentDay || Date.now() > getCurrentDay._nextDayCheck) {
		const day = getTime().substr(0, 8);
		if (day !== getCurrentDay._currentDay) getCurrentDay._nextDayCheck = Date.now() + 3600 * 1e3;
		getCurrentDay._currentDay = day;
	}
	return getCurrentDay._currentDay;
}
//#endregion
export { dateString, getCurrentDay, getTime, newUTCDate };
