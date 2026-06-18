import { getCurrentScope, getIsolationScope } from "../currentScopes.js";
import { GEN_AI_CONVERSATION_ID_ATTRIBUTE } from "../semanticAttributes.js";
import { spanToJSON } from "../utils/spanUtils.js";
import { defineIntegration } from "../integration.js";
//#region node_modules/@sentry/core/build/esm/integrations/conversationId.js
var INTEGRATION_NAME = "ConversationId";
var _conversationIdIntegration = (() => {
	return {
		name: INTEGRATION_NAME,
		setup(client) {
			client.on("spanStart", (span) => {
				const scopeData = getCurrentScope().getScopeData();
				const isolationScopeData = getIsolationScope().getScopeData();
				const conversationId = scopeData.conversationId || isolationScopeData.conversationId;
				if (conversationId) {
					const { op, data: attributes, description: name } = spanToJSON(span);
					if (!op?.startsWith("gen_ai.") && !attributes["ai.operationId"] && !name?.startsWith("ai.")) return;
					span.setAttribute(GEN_AI_CONVERSATION_ID_ATTRIBUTE, conversationId);
				}
			});
		}
	};
});
/**
* Automatically applies conversation ID from scope to spans.
*
* This integration reads the conversation ID from the current or isolation scope
* and applies it to spans when they start. This ensures the conversation ID is
* available for all AI-related operations.
*/
var conversationIdIntegration = defineIntegration(_conversationIdIntegration);
//#endregion
export { conversationIdIntegration };
