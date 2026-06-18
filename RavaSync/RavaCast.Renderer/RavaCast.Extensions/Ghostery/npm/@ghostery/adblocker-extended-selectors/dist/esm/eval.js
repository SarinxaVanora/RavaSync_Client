/*!
* Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/
(function() {
	const expressions = [];
	return function compile(query) {
		for (const [literal, expression] of expressions) if (query === literal) return expression;
		const expression = document.createExpression(query);
		expressions.push([query, expression]);
		return expression;
	};
})();
//#endregion
