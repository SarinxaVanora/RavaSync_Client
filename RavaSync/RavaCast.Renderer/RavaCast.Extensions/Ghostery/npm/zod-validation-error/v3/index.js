import { ZodIssueCode } from "../../zod/lib/index.js";
//#region node_modules/zod-validation-error/v3/index.mjs
function isZodErrorLike(err) {
	return err instanceof Error && err.name === "ZodError" && "issues" in err && Array.isArray(err.issues);
}
var ValidationError = class extends Error {
	name;
	details;
	constructor(message, options) {
		super(message, options);
		this.name = "ZodValidationError";
		this.details = getIssuesFromErrorOptions(options);
	}
	toString() {
		return this.message;
	}
};
function getIssuesFromErrorOptions(options) {
	if (options) {
		const cause = options.cause;
		if (isZodErrorLike(cause)) return cause.issues;
	}
	return [];
}
function isNonEmptyArray(value) {
	return value.length !== 0;
}
function stringifySymbol(symbol) {
	return symbol.description ?? "";
}
var identifierRegex = /[$_\p{ID_Start}][$\u200c\u200d\p{ID_Continue}]*/u;
function joinPath(path) {
	if (path.length === 1) {
		let propertyKey = path[0];
		if (typeof propertyKey === "symbol") propertyKey = stringifySymbol(propertyKey);
		return propertyKey.toString() || "\"\"";
	}
	return path.reduce((acc, propertyKey) => {
		if (typeof propertyKey === "number") return acc + "[" + propertyKey.toString() + "]";
		if (typeof propertyKey === "symbol") propertyKey = stringifySymbol(propertyKey);
		if (propertyKey.includes("\"")) return acc + "[\"" + escapeQuotes(propertyKey) + "\"]";
		if (!identifierRegex.test(propertyKey)) return acc + "[\"" + propertyKey + "\"]";
		return acc + (acc.length === 0 ? "" : ".") + propertyKey;
	}, "");
}
function escapeQuotes(str) {
	return str.replace(/"/g, "\\\"");
}
var ISSUE_SEPARATOR = "; ";
var MAX_ISSUES_IN_MESSAGE = 99;
var PREFIX = "Validation error";
var PREFIX_SEPARATOR = ": ";
var UNION_SEPARATOR = ", or ";
function createMessageBuilder(props = {}) {
	const { issueSeparator = ISSUE_SEPARATOR, unionSeparator = UNION_SEPARATOR, prefixSeparator = PREFIX_SEPARATOR, prefix = PREFIX, includePath = true, maxIssuesInMessage = MAX_ISSUES_IN_MESSAGE } = props;
	return (issues) => {
		return prefixMessage(issues.slice(0, maxIssuesInMessage).map((issue) => getMessageFromZodIssue({
			issue,
			issueSeparator,
			unionSeparator,
			includePath
		})).join(issueSeparator), prefix, prefixSeparator);
	};
}
function getMessageFromZodIssue(props) {
	const { issue, issueSeparator, unionSeparator, includePath } = props;
	if (issue.code === ZodIssueCode.invalid_union) return issue.unionErrors.reduce((acc, zodError) => {
		const newIssues = zodError.issues.map((issue2) => getMessageFromZodIssue({
			issue: issue2,
			issueSeparator,
			unionSeparator,
			includePath
		})).join(issueSeparator);
		if (!acc.includes(newIssues)) acc.push(newIssues);
		return acc;
	}, []).join(unionSeparator);
	if (issue.code === ZodIssueCode.invalid_arguments) return [issue.message, ...issue.argumentsError.issues.map((issue2) => getMessageFromZodIssue({
		issue: issue2,
		issueSeparator,
		unionSeparator,
		includePath
	}))].join(issueSeparator);
	if (issue.code === ZodIssueCode.invalid_return_type) return [issue.message, ...issue.returnTypeError.issues.map((issue2) => getMessageFromZodIssue({
		issue: issue2,
		issueSeparator,
		unionSeparator,
		includePath
	}))].join(issueSeparator);
	if (includePath && isNonEmptyArray(issue.path)) {
		if (issue.path.length === 1) {
			const identifier = issue.path[0];
			if (typeof identifier === "number") return `${issue.message} at index ${identifier}`;
		}
		return `${issue.message} at "${joinPath(issue.path)}"`;
	}
	return issue.message;
}
function prefixMessage(message, prefix, prefixSeparator) {
	if (prefix !== null) {
		if (message.length > 0) return [prefix, message].join(prefixSeparator);
		return prefix;
	}
	if (message.length > 0) return message;
	return PREFIX;
}
function fromZodError(zodError, options = {}) {
	if (!isZodErrorLike(zodError)) throw new TypeError(`Invalid zodError param; expected instance of ZodError. Did you mean to use the "${fromError.name}" method instead?`);
	return fromZodErrorWithoutRuntimeCheck(zodError, options);
}
function fromZodErrorWithoutRuntimeCheck(zodError, options = {}) {
	const zodIssues = zodError.errors;
	let message;
	if (isNonEmptyArray(zodIssues)) message = createMessageBuilderFromOptions2(options)(zodIssues);
	else message = zodError.message;
	return new ValidationError(message, { cause: zodError });
}
function createMessageBuilderFromOptions2(options) {
	if ("messageBuilder" in options) return options.messageBuilder;
	return createMessageBuilder(options);
}
var toValidationError = (options = {}) => (err) => {
	if (isZodErrorLike(err)) return fromZodErrorWithoutRuntimeCheck(err, options);
	if (err instanceof Error) return new ValidationError(err.message, { cause: err });
	return new ValidationError("Unknown error");
};
function fromError(err, options = {}) {
	return toValidationError(options)(err);
}
//#endregion
export { fromZodError };
