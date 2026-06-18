//#region node_modules/@adguard/logger/dist/es/index.mjs
/**
* Checks if error has message.
*
* @param error Error object.
*
* @returns True if error has message.
*/
function isErrorWithMessage(error) {
	return typeof error === "object" && error !== null && "message" in error && typeof error.message === "string";
}
/**
* Converts error to the error with a message.
*
* @param maybeError Possible error.
*
* @returns Error with a message.
*/
function toErrorWithMessage(maybeError) {
	if (isErrorWithMessage(maybeError)) return maybeError;
	try {
		return new Error(JSON.stringify(maybeError));
	} catch {
		return new Error(String(maybeError));
	}
}
/**
* Converts an error object to an error with a message. This method might be helpful to handle thrown errors.
*
* @param error Error object.
*
* @returns Message of the error.
*/
function getErrorMessage(error) {
	return toErrorWithMessage(error).message;
}
/**
* Pads a number with leading zeros.
*
* @param num The number to pad.
* @param size The number of digits to pad to.
*
* @returns The padded number.
*/
var pad = (num, size = 2) => {
	return num.toString().padStart(size, "0");
};
/**
* Formats a date into an ISO 8601-like string with milliseconds.
*
* @param {Date|number} date The date object or timestamp to format.
*
* @returns {string} The formatted date string.
*/
var formatTime = (date) => {
	const d = date instanceof Date ? date : new Date(date);
	return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}:${pad(d.getMilliseconds(), 3)}`;
};
/**
* String presentation of log levels, for convenient users usage.
* Ordered in the same way as LogLevelNumeric.
*
* First three levels will be shown to users, and the last two are for developers.
*/
var LogLevel;
(function(LogLevel) {
	/**
	* For errors.
	*/
	LogLevel["Error"] = "error";
	/**
	* For not critical errors.
	*/
	LogLevel["Warn"] = "warn";
	/**
	* For important information.
	* Use for general operational messages.
	*/
	LogLevel["Info"] = "info";
	/**
	* For debugging purposes, e.g. Inside conditions, loops or some edge cases.
	*/
	LogLevel["Debug"] = "debug";
	/**
	* For ultra-detailed, step-by-step traces (like stack traces or flow tracking).
	*/
	LogLevel["Verbose"] = "verbose";
})(LogLevel || (LogLevel = {}));
/**
* Log levels map, which maps number level to string level.
* Ordered in the same way as LogLevelNumeric.
*/
var levelMapNumToString = {
	[1]: LogLevel.Error,
	[2]: LogLevel.Warn,
	[3]: LogLevel.Info,
	[4]: LogLevel.Debug,
	[5]: LogLevel.Verbose
};
/**
* Log levels map, which maps string level to number level.
*/
var levelMapStringToNum = Object.entries(levelMapNumToString).reduce((acc, [key, value]) => {
	const numericKey = Number(key);
	if (!Number.isNaN(numericKey)) acc[value] = numericKey;
	return acc;
}, {});
/**
* Methods supported by console. Used to manage levels.
* Ordered in the same way as LogLevelNumeric.
*/
var LogMethod;
(function(LogMethod) {
	LogMethod["Error"] = "error";
	LogMethod["Warn"] = "warn";
	LogMethod["Info"] = "info";
	LogMethod["Debug"] = "debug";
	LogMethod["Trace"] = "trace";
})(LogMethod || (LogMethod = {}));
/**
* Simple logger with log levels.
*/
var Logger = class Logger {
	currentLevelValue = 3;
	writer;
	/**
	* Constructor.
	*
	* @param writer Writer object.
	*/
	constructor(writer = console) {
		this.writer = writer;
		this.error = this.error.bind(this);
		this.warn = this.warn.bind(this);
		this.info = this.info.bind(this);
		this.debug = this.debug.bind(this);
		this.trace = this.trace.bind(this);
	}
	/**
	* Print error messages.
	* Use when something went wrong.
	*
	* @param args Printed arguments.
	*/
	error(...args) {
		this.print(1, LogMethod.Error, args);
	}
	/**
	* Print warn messages.
	* Use when Something might go wrong.
	*
	* @param args Printed arguments.
	*/
	warn(...args) {
		this.print(2, LogMethod.Warn, args);
	}
	/**
	* Print messages you want to disclose to users.
	* Use for general operational messages.
	*
	* @param args Printed arguments.
	*/
	info(...args) {
		this.print(3, LogMethod.Info, args);
	}
	/**
	* Print debug messages. Usually used for technical information.
	*
	* @param args Printed arguments.
	*/
	debug(...args) {
		this.print(4, LogMethod.Debug, args);
	}
	/**
	* Print trace messages.
	* Ultra-detailed, step-by-step traces (like stack traces or flow tracking).
	*
	* @param args Printed arguments.
	*/
	trace(...args) {
		this.print(5, LogMethod.Trace, args);
	}
	/**
	* Getter for log level.
	*
	* @returns Logger level.
	*/
	get currentLevel() {
		return levelMapNumToString[this.currentLevelValue];
	}
	/**
	* Setter for log level. With this method log level can be updated dynamically.
	*
	* @param logLevel Logger level.
	*
	* @throws Error if log level is not supported.
	*/
	set currentLevel(logLevel) {
		const level = levelMapStringToNum[logLevel];
		if (level === void 0) throw new Error(`Logger supports only the following levels: ${[Object.values(LogLevel).join(", ")]}`);
		this.currentLevelValue = level;
	}
	/**
	* Converts error to string, and adds stack trace.
	*
	* @param error Error to print.
	*
	* @returns Error message.
	*/
	static errorToString(error) {
		return `${getErrorMessage(error)}\nStack trace:\n${error.stack}`;
	}
	/**
	* Prints error message with stack trace.
	* It prints the message with the stack trace in a collapsed group.
	* This is useful for debugging purposes, as it allows to see the stack trace
	* without cluttering the console with too many messages.
	*
	* @param formattedTime Formatted time.
	* @param formattedArgs Formatted arguments.
	*/
	printWithStackTrace(formattedTime, formattedArgs) {
		if (!this.writer.groupCollapsed || !this.writer.groupEnd) {
			this.writer.trace(formattedTime, ...formattedArgs);
			return;
		}
		this.writer.groupCollapsed(formattedTime, ...formattedArgs);
		this.writer.trace();
		this.writer.groupEnd();
	}
	/**
	* Wrapper over log methods.
	*
	* @param level Logger level.
	* @param method Logger method.
	* @param args Printed arguments.
	*/
	print(level, method, args) {
		if (this.currentLevelValue < level) return;
		if (!args || args.length === 0 || !args[0]) return;
		const formattedArgs = args.map((value) => {
			if (value instanceof Error) return Logger.errorToString(value);
			if (value && typeof value.message === "string") return value.message;
			if (typeof value === "object" && value !== null) return JSON.stringify(value);
			return String(value);
		});
		const formattedTime = `${formatTime(/* @__PURE__ */ new Date())}:`;
		/**
		* If current log level is Debug or Verbose, print all channels with stack
		* trace via using writer.trace method to help identify the location of the
		* log.
		*
		* Exception is Error method, because it is already contains build-in
		* stack trace.
		*/
		if (this.currentLevelValue >= levelMapStringToNum[LogLevel.Debug] && method !== LogMethod.Error) {
			this.printWithStackTrace(formattedTime, formattedArgs);
			return;
		}
		this.writer[method](formattedTime, ...formattedArgs);
	}
};
//#endregion
export { Logger };
