var compact_rules_default = {
	v: 1,
	s: [
		"dialog.cookie-consent",
		"dialog.cookie-consent form.cookie-consent__form",
		"dialog.cookie-consent form.cookie-consent__form button[value=no]",
		"dialog.cookie-consent form.cookie-consent__form button.cookie-consent__options-toggle",
		"dialog.cookie-consent form.cookie-consent__form button[value=\"save_options\"]",
		"div.acris-cookie-consent",
		"[data-acris-cookie-consent]",
		".acris-cookie-consent.is--modal",
		"#ccAcceptOnlyFunctional",
		"#cookie-banner",
		"#adopt-controller-button",
		"#adopt-reject-all-button",
		"#adroll_consent_container",
		"#adroll_consent_reject",
		"__adroll_fpc",
		".c-cookie-notice button[data-qa='allow-all-cookies']",
		".c-cookie-notice",
		"button[data-qa=\"reject-non-essentials\"]",
		"serif_manage_cookies_viewed",
		"serif_allow_analytics",
		"#sp-cc-wrapper,[data-action=sp-cc],span[data-action=\"sp-cc\"][data-sp-cc*=\"rejectAllAction\"]",
		"#sp-cc-wrapper *,[data-action=sp-cc],span[data-action=\"sp-cc\"][data-sp-cc*=\"rejectAllAction\"]",
		"#sp-cc-rejectall-link",
		"#user-consent-management-granular-banner-overlay",
		"[data-testid=granular-banner-button-decline-all]",
		"div:has(> p > a[href='https://www.anthropic.com/legal/cookies'])",
		"div:has(> p > a[href='https://www.anthropic.com/legal/cookies']) button:nth-child(2)",
		"anthropic-consent-preferences",
		"#consent-tracking",
		"#consent-tracking .decline.btn",
		".modal-open bahf-cookie-disclaimer-dpl3",
		"bahf-cookie-disclaimer-dpl3",
		"bahf-cookie-disclaimer-dpl3:not([aria-hidden=true])",
		"cookie_consent=denied",
		"#cookie-policy-info,#cookie-policy-info-bg",
		"#cookie-policy-info button",
		"#cookie-policy-info .btn-reject",
		"#cookie-policy-info .btn-setting",
		"#cookie-policy-info .btn-ok",
		"form[class*=\"cookie-banner\"][method=\"post\"]",
		"form[class*=\"cookie-banner\"] div[class*=\"simple-options\"] a[class*=\"customize-button\"]",
		"input[type=checkbox][checked]:not([disabled])",
		"a[class*=\"accept-selection-button\"]",
		"#awsccc-cb-content",
		"#awsccc-cs-container",
		"#awsccc-cs-modalOverlay",
		"#awsccc-cs-container-inner",
		"button[data-id=awsccc-cb-btn-customize]",
		"input[aria-checked]",
		"input[aria-checked=true]",
		"button[data-id=awsccc-cs-btn-save]",
		".axeptio_widget,.axeptio_mount",
		".axeptio-widget--open",
		".axeptio_mount .axeptio_widget",
		".axeptio_mount .needsclick",
		"button#axeptio_btn_dismiss,button.ax-discardButton",
		"axeptio_authorized_vendors=%2C%2C",
		".b-cookie.is-open",
		".cookie-alert.t-dark",
		"#cookie-accept-necessary",
		".cookie-alert .checkbox__input:checked:not(:disabled)",
		".cookie-alert__button button",
		".cookies-message",
		".cookies-message [name='allow-all']",
		".cookies-message button.bds-button-unstyled",
		".cookies-policy-dialog[open]",
		".cookies-policy-dialog fieldset input[type=radio][value=false]:not(:checked)",
		".cookies-policy-dialog button.bds-button",
		".cookies-message button.bds-button:not([name])",
		"#consent-manager",
		"#consent-manager button",
		"#consent-manager button:nth-child(2)",
		"tracking-preferences",
		"#bnp_container",
		"#bnp_cookie_banner",
		"#bnp_btn_accept,#bnp_btn_reject",
		"#bnp_btn_reject",
		"AD=0",
		".cookie-notification",
		"#blocksy-ext-cookies-consent-styles-css",
		".cookie-notification .ct-cookies-decline-button",
		"blocksy_cookies_consent_accepted=no",
		"#BorlabsCookieBox",
		"._brlbs-block-content,.brlbs-cmpnt-dialog",
		"._brlbs-bar-wrap,._brlbs-box-wrap,.brlbs-cmpnt-dialog",
		".brlbs-btn-accept-only-essential,a[data-cookie-refuse]",
		"a[data-cookie-individual]",
		".cookie-preference",
		"input[data-borlabs-cookie-checkbox]:checked",
		"#CookiePrefSave",
		"#bsw-consentCookie",
		"#bsw-consentCookie .accept-btn",
		"#bsw-consentCookie .manage-cookies-btn",
		"#bsw-consentCookie .config-banner_section",
		"#bsw-consentCookie input[type=checkbox]:checked:not(:disabled)",
		"x-bsw-consentCookie",
		".bpa-cookie-banner",
		".bpa-cookie-banner .bpa-module-full-hero",
		".bpa-close-button",
		"cookie-allow-tracking=0",
		".cookie-modal",
		"#notice-cookie-block",
		"#html-body #notice-cookie-block",
		"#btn-cookie-manage",
		"#notice-cookie-block input:checked",
		"#btn-cookie-save",
		"#html-body #notice-cookie-block, #notice-cookie",
		".cassie-cookie-module",
		".cassie-pre-banner",
		"#cassie_pre_banner_text",
		".cassie-reject-all",
		".cc_banner-wrapper",
		".cc_banner",
		".cc-banner[data-cc-banner]",
		".cc-banner[data-cc-banner] button[data-cc-action=reject]",
		".cc-banner[data-cc-banner] button[data-cc-action=preferences]",
		".cc-preferences[data-cc-preferences]",
		".cc-preferences[data-cc-preferences] input[type=radio][data-cc-action=toggle-category][value=off]",
		".cc-preferences[data-cc-preferences] button[data-cc-action=reject]",
		".cc-preferences[data-cc-preferences] button[data-cc-action=save]",
		"[data-modal-content]:has([data-toggle-target^='cookie'])",
		"[data-toggle-target^='cookie']",
		"[data-cookie-dismiss-all]",
		"#cp-gdpr-choices",
		".gdpr-top-content > button",
		".gdpr-top-back",
		".gdpr-btm__right > button:nth-child(1)",
		"#ccc-module,#ccc-overlay,#ccc",
		"#ccc-module,#ccc-notify",
		"#ccc",
		".ccc-reject-button",
		"#ccc-module",
		"#ccc-dismiss-button",
		".cc-individual-cookie-settings",
		".cc-individual-cookie-settings #cookie-settings-reject-all",
		"ckies_cookielaw",
		"#cl-consent",
		"#cl-consent [data-role=\"b_options\"]",
		".cl-consent-popup.cl-consent-visible [data-role=\"alloff\"]",
		"[data-role=\"b_save\"]",
		"__lxG__consent__v2_daisybit=",
		".consent-modal[role=dialog]",
		"#consent_reject",
		"#manage_cookie_preferences",
		"#cookie_consent_preferences input:checked",
		"#consent_save",
		"ctc_rejected=1",
		".cf_modal_container",
		"#cmplz-cookiebanner-container",
		"#cmplz-cookiebanner-container .cmplz-cookiebanner",
		".cmplz-cookiebanner .cmplz-deny",
		"cmplz_banner-status=dismissed",
		".cc-type-categories[aria-describedby=\"cookieconsent:desc\"]",
		".cc-type-categories[aria-describedby=\"cookieconsent:desc\"] .cc-dismiss",
		".cc-dismiss",
		".cc-type-categories input[type=checkbox]:not([disabled]):checked",
		".cc-save",
		".cc-type-info[aria-describedby=\"cookieconsent:desc\"]",
		".cc-type-info[aria-describedby=\"cookieconsent:desc\"] .cc-compliance .cc-btn",
		".cc-deny",
		"[aria-describedby=\"cookieconsent:desc\"]",
		"[aria-describedby=\"cookieconsent:desc\"] .cc-type-opt-both",
		"[aria-describedby=\"cookieconsent:desc\"].cc-type-opt-out",
		".cmp-pref-link",
		".cmp-body [id*=rejectAll]",
		".cmp-body .cmp-save-btn",
		".cc-type-opt-in[aria-describedby=\"cookieconsent:desc\"]",
		".cc-settings",
		".cc-settings-view",
		".cc-settings-view input[type=checkbox]:not([disabled]):checked",
		".cc-settings-view .cc-btn-accept-selected",
		"csm-cookie-consent",
		"#cookie-information-template-wrapper",
		"#CcpaWrapper",
		"CookieInformationConsent=",
		".spicy-consent-wrapper",
		".spicy-consent-bar",
		".js-decline-all-cookies",
		"#cookie-law-info-bar, #cookie-law-bg, .cli-popupbar-overlay",
		"#cookie-law-info-bar",
		"cookielawinfo-checkbox-non-necessary=yes",
		"#btn-cookie-settings",
		"#notice-cookie-block #allow-functional-cookies, #notice-cookie-block #btn-cookie-settings",
		"#allow-functional-cookies",
		".modal-body",
		".modal-body input:checked, .switch[data-switch=\"on\"]",
		"[role=\"dialog\"] .modal-footer button",
		"#cookiescript_injected",
		"#cookiescript_reject",
		"#cookiescript_manage",
		".cookiescript_fsd_main",
		"#cookieAcceptBar.cookieAcceptBar",
		"#cc--main",
		"html.show--consent #cm",
		"#s-all-bn",
		"#s-rall-bn",
		"#c-s-bn",
		"#s-sv-bn",
		"cc_cookie=",
		"#cc-main",
		"#cc-main .cm-wrapper",
		".cm__btn[data-role=necessary]",
		".cm__btn[data-role=show]",
		"#cc-main .pm__btn[data-role=necessary]",
		".cc-cookies",
		".cc-cookies .cc-cookie-accept",
		".cc-cookies .cc-cookie-decline",
		"#cookiefirst-root,.cookiefirst-root,[aria-labelledby=cookie-preference-panel-title]",
		"#cookiefirst-root,.cookiefirst-root",
		"button[data-cookiefirst-action=adjust]",
		"[data-cookiefirst-widget=modal]",
		"button[data-cookiefirst-action=save]",
		"button[data-cookiefirst-action=reject]",
		".ch2-container",
		".ch2-dialog",
		".ch2-open-settings-btn, .ch2-open-personal-data-btn",
		".ch2-settings",
		".ch2-deny-all-btn",
		".ch2-settings input[type=checkbox]:not([disabled]):checked",
		".ch2-save-settings-btn",
		"cookiehub=",
		".cookiejs-banner-wrapper",
		".cookiejs-banner-wrapper:not(.modal)",
		".cookiejs-banner-wrapper button.cookiejs-button",
		".cookiejs-banner-wrapper #reject-cookies-btn",
		"analytics_cookies=false",
		".cookiejs-banner-wrapper.modal",
		".cookiejs-banner-wrapper .cookiejs-banner-text",
		".cookiejs-banner-wrapper .cookiejs-banner-li:nth-of-type(2) button",
		"cookiejs_preferences",
		".cky-overlay,.cky-consent-container",
		".cky-consent-container",
		".cky-consent-container [data-cky-tag=reject-button]",
		".cky-consent-container [data-cky-tag=settings-button]",
		".cky-modal-open input[type=checkbox],.cky-consent-bar-expand input[type=checkbox]",
		".cky-modal-open input[type=checkbox]:checked,.cky-consent-bar-expand input[type=checkbox]:checked",
		".cky-modal [data-cky-tag=detail-save-button],.cky-consent-bar-expand [data-cky-tag=detail-save-button]",
		".cky-consent-container,.cky-overlay,.cky-consent-bar-expand",
		"advertisement:no",
		".cookiealert",
		".configurecookies",
		".confirmcookies",
		"#ct-ultimate-gdpr-cookie-popup",
		"#ct_ultimate-gdpr-cookie-reject a,#ct-ultimate-gdpr-cookie-reject",
		"#ct-ultimate-gdpr-cookie-change-settings",
		"#ct-ultimate-gdpr-cookie-modal-slider-form input[type=radio][value=\"1\"], #ct-ultimate-gdpr-cookie-modal-slider-form input[type=radio][value=\"2\"]",
		"#ct-ultimate-gdpr-cookie-modal-body .save",
		"ct-ultimate-gdpr-cookie=",
		"#cookiebar",
		"#cookiebar .cookiebar-content",
		"div[class*=\"CookiePopup__desktopContainer\"]:has(div[class*=\"CookiePopup\"])",
		"div[class*=\"CookiePopup__desktopContainer\"]",
		".cookie-banner.show .cookie-banner__content-all-btn",
		".cookie-banner__content-essential-btn",
		"*[data-testid='dl-cookieBanner']",
		"[data-testid='dl-cookieBanner']",
		"button[data-testid='cookie-banner-strict-accept-selected']",
		"#didomi-popup,.didomi-popup-container,.didomi-popup-notice,.didomi-consent-popup-preferences,#didomi-notice,.didomi-popup-backdrop,.didomi-screen-medium",
		"#didomi-host",
		"#didomi-popup, #didomi-notice, .didomi-popup-notice, .didomi-notice-banner, #didomi-host:not(:empty)",
		"#didomi-notice-disagree-button,.didomi-continue-without-agreeing",
		"[data-project=\"mol-fe-cmp\"]",
		"[data-project=\"mol-fe-cmp\"] [class*=footer]",
		"[data-project=\"mol-fe-cmp\"] button[class*=basic]",
		"[data-project=\"mol-fe-cmp\"] div[class*=\"tabContent\"]",
		"[data-project=\"mol-fe-cmp\"] div[class*=\"toggle\"][class*=\"enabled\"]",
		"#mol-ads-cmp-iframe, div.mol-ads-cmp > form > div",
		"div.mol-ads-cmp > form > div",
		"div.mol-ads-ccpa--message > u > a",
		".mol-ads-cmp--modal-dialog",
		"a.mol-ads-cmp-footer-privacy",
		"button.mol-ads-cmp--btn-secondary",
		"#pg-root-shadow-host",
		"#drupalorg-crosssite-gdpr",
		".no",
		"div[data-testid=cookie-consent-modal-backdrop]",
		"div[data-testid=cookie-consent-message-contents]",
		"button[data-testid=cookie-consent-adjust-settings]",
		"button[data-testid=cookie-consent-preferences-save]",
		"cc_functional=0",
		"cc_targeting=0",
		"div[role='dialog'][aria-modal='true']:has([data-qa='accept-all-button'])",
		"[data-qa='accept-all-button']",
		"[data-qa='reject-non-essential-button']",
		"#ensNotifyBanner",
		".ensModal",
		"#ensNotifyBanner[style*=block]",
		"#ensModalWrapper[style*=block]",
		"#ensNotifyBanner #ensRejectAll,#ensNotifyBanner #rejectAll,#ensNotifyBanner #ensRejectBanner,#ensNotifyBanner .rejectAll,#ensNotifyBanner #bannerRejectButton,#ensNotifyBanner #ensRejectAds",
		"#ensOpenModal,#modalOpenButton,#bannerOpenModal",
		".ensCheckbox:checked:not(:disabled)",
		"#ensSave,#modalSaveButton,#bannerSave",
		"#ensCloseBanner",
		"#gdpr-single-choice-overlay",
		"#gdpr-privacy-settings",
		"button[data-gdpr-open-full-settings]",
		".gdpr-overlay-body input",
		".pea_cook_wrapper,.pea_cook_more_info_popover",
		".pea_cook_wrapper",
		"euCookie",
		"body.eu-cookie-compliance-popup-open",
		".eu-cookie-compliance-banner .decline-button, .eu-cookie-compliance-banner .accept-necessary, .eu-cookie-compliance-save-preferences-button",
		"cookie-agreed=2",
		"#ez-cookie-dialog-wrapper",
		"#ez-manage-settings",
		"#ez-cookie-dialog input[type=checkbox]",
		"#ez-cookie-dialog input[type=checkbox]:checked",
		"#ez-save-settings",
		"ez-consent-tcf",
		"#fast-cmp-root",
		"iframe#fast-cmp-iframe",
		"fastCMP-",
		"fedex-gdpr",
		"fedex-gdpr div",
		"fedex-gdpr .fxg-gdpr__reject-all-btn",
		"_svs=",
		"#fides-overlay, #fides-overlay-wrapper",
		"#fides-overlay-wrapper #fides-banner",
		".fides-banner-button",
		"#fides-banner .fides-reject-all-button",
		"#fides-banner .fides-manage-preferences-button",
		".fides-reject-all-button",
		"[fs-consent-element=banner]",
		"[fs-consent-element=banner] [fs-consent-element=allow]",
		"[fs-consent-element=banner] [fs-consent-element=deny]",
		".fc-consent-root,.fc-dialog-container,.fc-dialog-overlay,.fc-dialog-content",
		".fc-consent-root",
		".fc-dialog-container",
		".fc-cta-do-not-consent,.fc-cta-manage-options",
		".fc-preference-consent:checked,.fc-preference-legitimate-interest:checked",
		".fc-confirm-choices",
		"[class*=\"gcb-modal-overlay\"]",
		"[data-gcb-modal-show-prefs]",
		"[data-gcb-modal-save]",
		"gcbcl=a3|m3",
		".overlay_bc_banner",
		"a[href=\"https://gdpr-legal-cookie.myshopify.com/\"]",
		".overlay_bc_banner .banner-body",
		".overlay_bc_banner *[data-cookie-save]",
		"a[href^=\"https://policies.google.com/technologies/cookies\"",
		"form[action^=\"https://consent.google.\"][action$=\"/save\"],form[action^=\"https://consent.youtube.\"][action$=\"/save\"]",
		"form[action^=\"https://consent.google.\"][action$=\"/save\"]:has(input[name=set_eom][value=true]) button,form[action^=\"https://consent.youtube.\"][action$=\"/save\"]:has(input[name=set_eom][value=true]) button",
		".glue-cookie-notification-bar",
		".glue-cookie-notification-bar__reject",
		".HTjtHe#xe7COe",
		".HTjtHe#xe7COe a[href^=\"https://policies.google.com/technologies/cookies\"]",
		".HTjtHe#xe7COe button#W0wltc",
		"SOCS=CAE",
		".govuk-cookie-banner__message",
		".govuk-cookie-banner__message .govuk-button",
		".gravitoCMP-background-overlay",
		"#modalSettingBtn.gravitoCMP-button",
		"#allRejectBtn",
		".gravitoCMP-content-section",
		".gravitoCMP-content input[type=checkbox]:checked",
		"gravitoData",
		"#modal-host > div.no-hash > div.window-wrapper",
		"#modal-host > div.no-hash > div.window-wrapper, div[data-testid=qualtrics-container]",
		"#modal-host > div.no-hash > div.window-wrapper > div:last-child a[href=\"/privacy-settings\"]",
		"div#__next",
		"#__next div:nth-child(1) > button:first-child",
		".cookie-modal .cookie-accept-btn",
		".cookie-modal .js-cookie-reject-btn",
		"cookies_rejected=1",
		".cookieModalContent",
		"#cookie-banner-overlay",
		"#manageCookie",
		".cookieSettingsModal",
		"#AOCookieToggle",
		"#AOCookieToggle[aria-pressed=true]",
		"#TPCookieToggle",
		"#TPCookieToggle[aria-pressed=true]",
		"#updateCookieButton",
		"dialog[data-cookie-consent]",
		"a[data-cookie-accept=\"functional\"]",
		"#hu.hu-wrapper",
		"#hu.hu-visible",
		"#hu-cookies-save",
		"#hs-eu-cookie-confirmation",
		"#hs-eu-decline-button",
		".accept-cookie",
		".accept-cookie > .accept-cookie-inner",
		"xpath///*[contains(@class, 'accept-cookie')]//*[contains(text(), 'Decline')]",
		"cookie-pref=rejected",
		".privacy-consent--backdrop",
		".privacy-consent--modal",
		".footer-config-link",
		"#confirmSelection",
		"#iubenda-cs-banner",
		".iubenda-cs-accept-btn",
		".iubenda-cs-reject-btn",
		".iubenda-cs-customize-btn",
		".iub-btn-reject",
		"#iubFooterBtn",
		"body.cookies-request #cookie-bar",
		"body.cookies-request #cookie-bar .disallow-cookies",
		"cookie_permission_granted=no",
		".widget_eu_cookie_law_widget",
		"div[class^=pecr-cookie-banner-]",
		"button[data-test^=manage-cookies]",
		"label[data-test^=toggle][class*=checked]:not([class*=disabled])",
		"button[data-test=save-preferences]",
		".cookie-bar",
		".cookie-bar .cookie-bar__message,.cookie-bar .cookie-bar__buttons",
		"cookies-state=accepted",
		".consent-banner",
		".consent-banner .consent-banner__actions",
		".consent-banner__actions button.basic-button.secondary",
		".consent-modal__footer button.basic-button.secondary",
		".consent-modal ion-content > div > a:nth-child(9)",
		"label.consent-switch input[type=checkbox]:checked",
		".consent-modal__footer button.basic-button.primary",
		".kc-overlay",
		"#kconsent",
		".kc-dialog",
		"#kc-denyAndHide",
		"#lanyard_root div[role='dialog']",
		"#ketch-banner",
		"#ketch-modal",
		"#lanyard_root [class*='ketch-fixed']",
		"#ketch-banner-button-tertiary",
		"#lanyard_root [aria-describedby=banner-description]",
		"#lanyard_root div[class*=buttons] > button[class*=secondaryButton], #lanyard_root button[class*=buttons-secondary]",
		"#lanyard_root [id^='ketch-banner-buttons-container'] > button:only-child",
		"#lanyard_root [aria-describedby=preference-description],#lanyard_root [aria-describedby=modal-description], #ketch-preferences, #ketch-purposes-modal",
		"#lanyard_root button[class*=rejectButton], #lanyard_root button[class*=rejectAllButton], #ketch-modal button[aria-label='Reject All'], #ketch-purposes-modal button[aria-label='Reject All']",
		"#lanyard_root button[class*=confirmButton], #lanyard_root div[class*=actions_] > button:nth-child(1), #lanyard_root button[class*=actionButton], #ketch-modal button[aria-label='Confirm'], #ketch-purposes-modal button[aria-label='Confirm']",
		"_ketch_consent_v1_",
		".lia-cookie-banner-alert",
		".lia-cookie-banner-alert .AjaxFeedback",
		".lia-cookie-banner-alert a.lia-cookie-banner-alert-reject",
		".darken-layer.open,.lightbox.lightbox--cookie-consent",
		"body.cookie-consent-is-active div.lightbox--cookie-consent > div.lightbox__content > div.cookie-consent[data-jsb]",
		".cookie-consent__footer > button[type='submit']:not([data-button='selectAll'])",
		"#lgcookieslaw_banner,#lgcookieslaw_modal,.lgcookieslaw-overlay",
		".artdeco-global-alert[type=COOKIE_CONSENT]",
		".artdeco-global-alert[type=COOKIE_CONSENT] button[action-type=DENY]",
		"#macaron_cookie_box",
		"#macaron_cookie_box .macaronbtn",
		".macaronbtn.refuse",
		".macaronbtn.letmechoose",
		".macaronbtn.letmechoose.open",
		"#cookie_description .paragraph",
		"#cookie_description input[type=checkbox]:checked:not(:disabled)",
		".macaronbtn.confirmselection",
		"_deCookiesConsent",
		"div[aria-labelledby=pwa-consent-layer-title]",
		"div[class^=StyledConsentLayerWrapper-]",
		"div[aria-labelledby^=pwa-consent-layer-title]",
		"button[data-test^=pwa-consent-layer-deny-all]",
		"#wcpConsentBannerCtrl",
		"dialog[data-testid=accept-our-cookies-dialog]",
		"#banner-manage",
		"#pc-confirm",
		"#moove_gdpr_cookie_info_bar",
		"#moove_gdpr_cookie_info_bar:not(.moove-gdpr-info-bar-hidden)",
		".moove-gdpr-infobar-reject-btn",
		"#moove_gdpr_cookie_info_bar .change-settings-button",
		"#moove_gdpr_cookie_modal",
		".moove-gdpr-modal-save-settings",
		"#nhsuk-cookie-banner",
		"#nhsuk-cookie-banner__link_accept",
		".disc-cp--active",
		".disc-cp-modal__modal",
		".js-disc-cp-deny-all",
		".tx-om-cookie-consent",
		".tx-om-cookie-consent .active[data-omcookie-panel]",
		"[data-omcookie-panel-save=min]",
		"input[data-omcookie-panel-grp]:checked:not(:disabled)",
		"[data-omcookie-panel-save=save]",
		".legalmonster-cleanslate",
		".legalmonster-cleanslate #lm-cookie-wall-container",
		"#lm-accept-necessary",
		".osano-cm-window,.osano-cm-dialog",
		".osano-cm-window",
		".osano-cm-dialog:not(.osano-cm-dialog--hidden)",
		".osano-cm-denyAll,.osano-cm-deny",
		".cookieBanner--visibility",
		".cookieBanner__wrapper",
		".js_cookieBannerProhibitionButton",
		".cookie-banner:has([data-ol-cookie-banner-set-consent])",
		"[data-ol-cookie-banner-set-consent]",
		"[data-ol-cookie-banner-set-consent=essential]",
		"oa",
		".js-cookie-notice:has(#cookie_settings-form)",
		".js-cookie-notice #cookie_settings-form",
		".js-cookie-notice button[value=disable]",
		"#pandectes-banner",
		"#pandectes-banner .cc-deny",
		"#pandectes-banner .cc-settings",
		".pd-cp-ui-rejectAll",
		".pd-cp-ui-save",
		"#ccpaCookieContent_wrapper, article.ppvx_modal--overpanel",
		"#ccpaCookieBanner, .privacy-sheet-content",
		"#bannerDeclineButton",
		"a#manageCookiesLink",
		".privacy-sheet-content #formContent",
		"#formContent .cookiepref-11m2iee-checkbox_base input:checked",
		".cookieAction.saveCookie,.confirmCookie #submitCookiesBtn",
		"#gdprCookieBanner",
		"#gdprCookieContent_wrapper",
		"cookie_prefs",
		"#pmc-pp-tou--notice",
		".cookiesBanner",
		".cookiesBanner .wrapper-cookies",
		".gdpr-wrapper",
		".gdpr-wrapper .gdpr-cookie-wrapper",
		"#cookie-bar",
		"#cookie-bar .cb-enable,#cookie-bar .cb-disable,#cookie-bar .cb-policy",
		"#cookie-bar .cb-disable",
		"cb-enabled=accepted",
		"#cookie-consent-banner",
		"#cookie-consent-banner #accept-button",
		"#cookie-consent-banner #deny-button",
		"#cookie-consent-banner #manage-settings-button",
		"#manage-cookies #save-button",
		"#pubtech-cmp",
		"#pubtech-cmp #pt-actions",
		"#pubtech-cmp #pt-close",
		"#qc-cmp2-main,#qc-cmp2-container",
		"#qc-cmp2-container",
		"#qc-cmp2-ui",
		"#disagree-btn",
		".qc-cmp2-summary-buttons > button[mode=\"secondary\"]",
		".qc-cmp2-summary-buttons > button[mode=\"secondary\"]:nth-of-type(2)",
		".qc-cmp2-summary-buttons > button[mode=\"secondary\"]:nth-of-type(1)",
		"#qc-cmp2-ui .qc-cmp2-consent-info",
		".qc-cmp2-toggle-switch > button[aria-checked=\"true\"]",
		".qc-cmp2-buttons-desktop > button[mode=primary]",
		"div[consent-skip-blocker=\"1\"][id][data-bg]",
		"div[consent-skip-blocker=\"1\"][id][data-bg] > dialog > div > div > div > div > div > a[role=button][id]",
		".consent-banner .consent-banner-copy",
		".consent-banner .consent--manage",
		".consent-modal .consent-bucket",
		".consent-modal input[type=checkbox]:checked",
		".consent-modal .consent-save",
		".cookies-consent",
		".cookies-consent .cookies-inner",
		"s4s-privacy-module",
		"s4s-privacy-module button.decline",
		"_cookieanalytics",
		"#shopify-pc__banner",
		"#shopify-pc__banner__btn-decline",
		"sibbo-cmp-layout",
		"#rejectAllMain",
		"#sd-cmp",
		"#sd-cmp [role='button'][title='Close']",
		".snigel-cmp-framework",
		"#sn-b-custom",
		"#sn-b-save",
		"snconsent",
		".cookie-banner-mount-point",
		".cookie-banner-mount-point section[aria-label='Cookie banner']",
		".cookie-banner-mount-point button.decline",
		"#cookie-consent-banner #accept-cdp-cookie",
		"#reject-cdp-cookie",
		"squiz.cdp.consent",
		".cookiepreferences_popup",
		"#rejectAllButton",
		".cookies-reminder",
		".cookies-reminder .cookies-reminder__content",
		".cookies-reminder .cookies-reminder__manage-button",
		"dialog[open] .cookies-select-modal .cookies-select-modal__buttons .ds-btn-apply-2-ds",
		" c=%7B%22essential",
		"div[class*=cookieBanner].pencraft",
		"hideCookieBanner=",
		"#CookieConsentBannerPlaceholder",
		"#CookieConsentBannerPlaceholder .cookies-banner-container",
		".syno_cookie_element",
		".syno_cookie_element .btn_option",
		".syno_cookie_element .scb_dialog_body",
		".syno_cookie_element.scb_dialog input[type=checkbox]:checked:not(:disabled):not([readonly])",
		".syno_cookie_element.scb_dialog .scb_btn_save",
		"syno_confirm_v5_answer",
		"\"targeting\":false",
		"div[class^=\"cookies-banner-module_\"]",
		"div[class^=\"cookies-banner-module_cookie-banner_\"]",
		"div[class^=\"cookies-banner-module_small-cookie-banner_\"]",
		"#tarteaucitronRoot",
		"#tarteaucitronAllDenied2",
		"#tarteaucitronCloseAlert",
		".dsgvoaio-checkbox",
		".dsgvoaio-checkbox > input:checked",
		"#tarteaucitronPersonalize",
		"#tarteaucitronAllDenied",
		"#tarteaucitronClosePanel",
		"#taunton-user-consent__overlay",
		"#taunton-user-consent__overlay:not([aria-hidden=true])",
		"#taunton-user-consent__toolbar input[type=checkbox]:checked",
		"#taunton-user-consent__toolbar button[type=submit]",
		"taunton_user_consent_submitted=true",
		"#tccCmpAlert",
		"xpath///span[contains(.,'Accetta solo cookie necessari')]",
		"#__tealiumGDPRecModal,#__tealiumGDPRcpPrefs,#__tealiumImplicitmodal,#consent-layer",
		"#__tealiumGDPRecModal *,#__tealiumGDPRcpPrefs *,#__tealiumImplicitmodal *",
		"#__tealiumGDPRecModal,#__tealiumGDPRcpPrefs,#__tealiumImplicitmodal",
		"#cm-acceptNone,.js-accept-essential-cookies,#continueWithoutAccepting,#no_consent,#consent_prompt_decline",
		"#__tealiumGDPRecModal,#__tealiumGDPRcpPrefs",
		"#termly-code-snippet-support",
		"#termly-code-snippet-support div",
		"[data-tid=\"banner-decline\"]",
		".t-preference-button,[data-testid=\"preferences-link\"]",
		".t-declineAllButton",
		".t-preference-modal input[type=checkbox][checked]:not([disabled])",
		".t-saveButton",
		".termsfeed-com---nb",
		".cc-nb-reject",
		".cc-nb-changep",
		"input[cookie_consent_toggler=\"true\"]",
		"input[cookie_consent_toggler=\"true\"]:checked",
		".cc-cp-foot-save",
		".cc_dialog.cc_css_reboot,.cc_overlay_lock",
		".cc_dialog.cc_css_reboot",
		".cc_dialog.cc_css_reboot .cc_b_cp",
		".cookie-consent-preferences-dialog .cc_cp_f_save button",
		"#reject-all",
		"#privacy-test-page-cmp-test",
		"#privacy-test-page-cmp-test-prehide",
		"#privacy-test-page-cmp-test-banner",
		".consent-banner-box",
		"consent-banner[component=consent-banner]",
		"button[data-consent=disagree]",
		"#cmpBanner",
		"#cookie-banner.visible",
		"#cookie-banner.visible .cookie-banner__container .cookie-banner__accept",
		"#cookie-banner.visible .cookie-banner__container .cookie-banner__reject",
		"TOYOTANATIONAL_ENSIGHTEN_PRIVACY_TargetingCookies",
		".tp-dialog.cookie-dialog",
		".tp-dialog.cookie-dialog button",
		".tp-dialog-box .checkbox.clickable:not(.checked)",
		".tp-dialog-box .tp-cookie-save",
		"tp_privacy_base",
		"div.aem-page > div[class^=\"CookiesAlert_cookiesAlert__\"]",
		"#transcend-consent-manager",
		"#shopify-section-cookies-controller",
		"#shopify-section-cookies-controller #cookies-controller-main-pane",
		"#cookies-controller-main-pane a[data-tab-target=manage-cookies]",
		"#manage-cookies-pane.active",
		"#manage-cookies-pane.active input[type=checkbox][checked]:not([disabled])",
		"#manage-cookies-pane.active button[type=submit]",
		"#truyo-consent-module",
		"#truyo-cookieBarContent",
		"button#declineAllCookieButton",
		"#twcc__mechanism",
		"#twcc__mechanism .twcc__notice",
		"#twcc__decline-button",
		"twCookieConsent=",
		".u12-data-protection-notice",
		".u12-data-protection-notice button",
		".u12-data-protection-notice button.js-decline",
		"dialog.cookie-policy",
		"dialog.cookie-policy header",
		"xpath///*[@id=\"modal\"]/div/header",
		"dialog header",
		"button.js-manage",
		"xpath///*[@id=\"cookie-policy-content\"]/p[4]/button[2]",
		"dialog.cookie-policy .p-switch__input:checked",
		"dialog.cookie-policy .js-save-preferences",
		"xpath///*[@id=\"modal\"]/div/button",
		"_cookies_accepted=essential",
		"#catapult-cookie-bar",
		".has-cookie-bar #catapult-cookie-bar",
		"catAccCookies",
		"#usercentrics-root,#usercentrics-cmp-ui",
		"#usercentrics-cmp-ui",
		"#usercentrics-root",
		"#usercentrics-button",
		"#usercentrics-button #uc-btn-accept-banner",
		"#usercentrics-button #uc-btn-deny-banner",
		"div[aria-labelledby=CookieAlertModalHeading]",
		"section[data-test=initial-waitrose-cookie-consent-banner]",
		"section[data-test=cookie-consent-modal]",
		"button[data-test=manage-cookies]",
		"button[data-test=submit]",
		"wtr_cookies_advertising=0",
		"wtr_cookies_analytics=0",
		".fs-cc-components,[fs-cc=banner]",
		".fs-cc-components,[fs-cc=banner] [fs-cc=allow]",
		"[fs-cc=banner] [fs-cc=deny]",
		"[fs-cc=banner]",
		"fs-cc",
		".mw-cookiewarning-container",
		"[data-comp-type=cookie-banner-root-wix],[data-hook=ccsu-banner-wrapper]",
		"[data-hook=ccsu-banner-decline-all]",
		"[data-hook=ccsu-banner-wrapper]",
		".wccom-comp-privacy-banner .wccom-privacy-banner",
		".wccom-privacy-banner__content-buttons button.is-secondary",
		"div.wccom-modal__footer > button",
		"#gdpr-cookie-consent-bar",
		"#gdpr-cookie-consent-bar #cookie_action_reject",
		"wpl_viewed_cookie=no",
		".sp-dsgvo",
		".sp-dsgvo.sp-dsgvo-popup-overlay",
		".sp-dsgvo-privacy-btn-accept-nothing",
		"sp_dsgvo_cookie_settings",
		".wpcc-container",
		".wpcc-container .wpcc-message",
		".cookie_warn",
		".cookie_warn .cookie_warn_inner",
		"div[class^=cookie-consent-CookieConsent]",
		"#consent-settings-button",
		".consent-banner-button-accept-overlay",
		"userConsent=%7B%22marketing%22%3Afalse",
		"#cookies-use-alert",
		".euCookieModal, #js_euCookieModal",
		".euCookieModal",
		"tp-yt-iron-overlay-backdrop.opened",
		"ytd-consent-bump-v2-lightbox",
		"ytd-consent-bump-v2-lightbox tp-yt-paper-dialog",
		"ytd-consent-bump-v2-lightbox tp-yt-paper-dialog a[href^=\"https://consent.youtube.com/\"]",
		"ytd-consent-bump-v2-lightbox .eom-buttons .eom-button-row:first-child ytd-button-renderer:first-child #button,ytd-consent-bump-v2-lightbox .eom-buttons .eom-button-row:first-child ytd-button-renderer:first-child button",
		".consent-bump-v2-lightbox",
		"ytm-consent-bump-v2-renderer",
		"ytm-consent-bump-v2-renderer .privacy-terms + .one-col-dialog-buttons c3-material-button:nth-child(2) button, ytm-consent-bump-v2-renderer .privacy-terms + .one-col-dialog-buttons ytm-button-renderer:nth-child(2) button",
		"#zdf-cmp-banner-sdk",
		"#zdf-cmp-main.zdf-cmp-show",
		"#zdf-cmp-main #zdf-cmp-deny-btn",
		".cookie-alert-extended",
		".cookie-alert-extended-modal",
		"a[data-controller='cookie-alert/extended/detail-link']",
		".cookie-alert-configuration-input:checked",
		"button[data-controller='cookie-alert/extended/button/configuration']",
		"body > div#root > div#ccpa-iframe-theme-provider[data-testid=\"ccpa-iframe-theme-provider\"] > div#ccpa-iframe[data-testid=\"ccpa-iframe\"] > div#ccpa_consent_banner[data-testid=\"ccpa_consent_banner\"] > div:not([id]) > div:nth-child(3):not([id]) > span:not([id]) > span:not([id]) > div:nth-child(2):not([id]) > span:nth-child(1):not([id]) > button#decline_cookies_button[data-testid=\"decline_cookies_button\"]",
		"body:not([id]) > div#banner-0 > div:nth-child(1)#grouped-pageload-Banner > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div#iubenda-cs-banner > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(5):not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div#ds-cookie-consent-banner > div:not([id]) > div:not([id]) > button:nth-child(4)#reject_optional_cookies",
		"body:not([id]) > div#banner-0 > div:nth-child(1)#grouped-pageload-Banner > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(3):not([id])",
		"body > div:not([id]) > div#clym-app-layout > div:nth-child(2)#clym-notice-layout > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(1):not([id])",
		"body > div:not([id]) > div#clym-app-layout > div:nth-child(2)#clym-notice-layout > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(1):not([id])",
		"body > div:not([id]) > div#clym-app-layout > div:nth-child(2)#clym-notice-layout > div:nth-child(2):not([id]) > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(1)#handle-reject-all",
		"body > div#banner-0 > div:nth-child(1)#grouped-pageload-Banner > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(1)#dashboard > div:not([id]) > div:nth-child(2)#dashboard-body-container > div:nth-child(2):not([id]) > button:nth-child(4)#decline-text",
		"body > div:not([id]) > div:nth-child(4):not([id]) > button#cookie_all_reject",
		"body > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2)#cookie_all_reject",
		"body > div:not([id]) > div#clym-app-layout > div:nth-child(2)#clym-notice-layout > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(1)#handle-reject-all",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(1)#dashboard > div:not([id]) > div:nth-child(1):not([id]) > button#decline-text",
		"body:not([id]) > div#root > div#ccpa-iframe-theme-provider > div#ccpa-iframe > div#ccpa_consent_banner > div:not([id]) > div:nth-child(3):not([id]) > span:not([id]) > span:not([id]) > div:nth-child(2):not([id]) > span:nth-child(1):not([id]) > button#decline_cookies_button",
		"body#moneyinvesting > div#top > div#content > div:nth-child(4):not([id]) > a:nth-child(11):not([id])",
		"body > div#banner-0 > div:nth-child(1)#grouped-pageload-Banner > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(3):not([id])",
		"body > div#app > div#app > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:not([id]) > button:nth-child(1):not([id]) > div:not([id])",
		"body > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(4):not([id]) > div:not([id]) > a:nth-child(1)#ipoclick-btn-cookiebar-ik_ga_niet_akkoord_var_a",
		"body > div[id] > div > div > div > div > div:nth-child(3) > span > span > div:nth-child(2) > span:nth-child(1) > button",
		"div[class^=CookieBannerContent__Container]",
		"button[class^=CookieBannerContent__Settings]",
		"div[class^=CookiePreferencesModal__CategoryContainer] input:checked",
		"div[class^=CookiePreferencesModal__ButtonContainer] > button",
		"#gdpr-consent-tool-wrapper",
		"iframe[src^=\"https://cmp-consent-tool.privacymanager.io\"]",
		"button#save",
		"#denyAll",
		".okButton",
		"#manageSettings",
		".purposes-overview-list",
		"button#saveAndExit",
		"span[role=checkbox][aria-checked=true]",
		"#save-all-pur",
		"#reminder",
		"[data-component=CookieBanner]",
		"[data-component=CookieBanner] [data-component=CookieBanner_AcceptAll]",
		"[data-component=CookieBanner] [data-component=CookieBanner_AcceptABCRequired]",
		"trackingconsent",
		"aside#cookies,.overlay-cookies",
		"#cookies .cookies-btn",
		"#cookies #submitCookies",
		"#cookies #rejectCookies",
		".fullpageCover",
		".fullpageCover a[href*='/go/page/privacy.html']",
		".fullpageCover div.rounded-full:nth-child(2)",
		"#cookie-overlay",
		"#decline-cookies",
		"#aag-cookie-consent",
		"#gdpr-new-container",
		"#gdpr-new-container,#voyager-gdpr > div",
		"#voyager-gdpr > div",
		"#voyager-gdpr > div > div > button:nth-child(2)",
		"#gdpr-new-container .btn-more",
		"#gdpr-new-container .gdpr-dialog-switcher",
		"#gdpr-new-container .switcher-on",
		"#gdpr-new-container .btn-save",
		"div:has(> div > button[allytmln=ally-cookie-consent])",
		"button[allytmln=ally-cookie-consent]",
		"#footer-container ~ div",
		"#footer-container > div",
		"body > div#shopify-section-popups > div:nth-child(1):not([id]) > modal-box#modal-popups-0 > div:nth-child(1):not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div#cookieNotice > span:not([id])",
		"body > div#cookieAreaBase > div#cookieArea > p:nth-child(1):not([id]) > a:nth-child(3):not([id])",
		"body > div#cookieAreaBase > div#cookieArea > div:nth-child(2):not([id]) > a:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(4):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body#customcss > div#root > div:nth-child(4):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:not([id]) > span:nth-child(1):not([id]) > div:nth-child(5):not([id]) > button:nth-child(2):not([id])",
		"body > div#cookieConsentModal > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:not([id]) > button:nth-child(2):not([id])",
		"body > div#cookie-banner-deezer > div#gdpr-dir-tag > div:not([id])[data-testid=\"cookie-banner\"] > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button#gdpr-btn-refuse-all[data-testid=\"gdpr-btn-refuse-all\"]",
		"body:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body:not([id]) > div:not([id]) > div#gdpr_notification_container > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:nth-child(6):not([id]) > div:not([id]) > a#gdpr_reject_button",
		"#cookiesPortletDiv",
		"#cookiesPortletDiv .cookie-buttons",
		"#cookiesPortletDiv button[aria-label='Only Mandatory']",
		"body > dialog:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > button:nth-child(2)#deny-consent-button",
		"body > div#cookie-policy > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		".MuiStack-root:has(a[href='/privacy']):has(button)",
		"body > div#portal-cookie-banner > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2)#portal-cookie-banner__wrapper > aside#portal-cookie-banner__content > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#messages > div:nth-child(1)#shopify-section-cookie-banner > section#shopify-section-cookie-banner > div:nth-child(1):not([id]) > button:nth-child(2):not([id])",
		"body > div#messages > div:nth-child(2)#shopify-section-newsletter-banner > section:nth-child(1):not([id]) > div:not([id]) > button:nth-child(1):not([id])",
		"body > form#trackingConsent > button:nth-child(2):not([id])",
		"body:not([id]) > div#cookieConsent > div:nth-child(2):not([id]) > a:nth-child(3):not([id])",
		"body > pnp-root:not([id]) > div:not([id]) > ui-storefront:not([id]) > footer:nth-child(8):not([id]) > cx-page-layout:not([id]) > cx-page-slot:not([id]) > cms-popia-component:nth-child(3):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div#___gatsby > div:nth-child(1)#gatsby-focus-wrapper > div:nth-child(3):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(1):not([id]) > button:not([id])",
		"body > div#layout-fea-0-bd-5-e-e-723-4027-aa-78-dbcc-846073-a-6 > div#page-62928 > div:not([id]) > div:nth-child(8)#\\31 e9783d2-3575-4c86-b6e8-bc0fd6b17307 > div#\\31 e9783d2-3575-4c86-b6e8-bc0fd6b17307-banner > div:nth-child(3):not([id]) > a:nth-child(1)#\\31 e9783d2-3575-4c86-b6e8-bc0fd6b17307-decline",
		"body > div:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div#c-cookiebanner > section:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(4):not([id]) > p:nth-child(1):not([id]) > button:not([id])",
		"body > div#wrapper > div:nth-child(4):not([id]) > button:nth-child(3):not([id])",
		"body > div#frame-modals > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div#message-banner > div:not([id]) > div:nth-child(2):not([id]) > button:not([id])",
		"body > div#cookiemodal > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > div:nth-child(5)#footer-cookie-buttons > a:nth-child(2)#footer-cookie-close",
		"body > div#notice-cookie-block > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:not([id])",
		"body > div#termsfeed-pc1-notice-banner > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:not([id]) > button:nth-child(2)#termsfeed_privacy_consent_banner_button_reject_all",
		"body > main:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > button:nth-child(2):not([id])",
		"body > div#accn-cookie-consent-wrapper > div#accn-cookie-consent > div:nth-child(2)#accn-statement-scroller > div:nth-child(2):not([id]) > div:nth-child(3):not([id]) > button:not([id])",
		"body:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(1):not([id]) > button:nth-child(2):not([id])",
		"body > hathi-cookie-consent-banner:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#cookies-modal > div:nth-child(2)#consent-modal > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(1):not([id]) > span#consent-modal-refuse",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div#csm-wrapper > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#root > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(1):not([id]) > button:not([id])",
		"body > div#body-wrapper > section:nth-child(5):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(2):not([id])",
		"body > div#wrapwrap > div:nth-child(5)#website_cookies_bar > div:not([id]) > div:not([id]) > div:not([id]) > section:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > a:nth-child(1)#cookie-banner-essential",
		"body > aside:not([id]) > div:nth-child(2)#cookie-consent-modal > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body:not([id]) > div#cc--main > div#cc_div > div:nth-child(1)#cm > div#c-inr > div:nth-child(2)#c-bns > button:nth-child(2)#c-s-bn",
		"body > div#dk-cookie-message > div:not([id]) > button:nth-child(3):not([id])",
		"body > aside:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button:nth-child(3):not([id])",
		"body:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > a:nth-child(3):not([id])",
		"body > div:not([id]) > div:not([id]) > div#ch2-settings-dialog > div:nth-child(3):not([id]) > div:nth-child(1)#ch2-settings > button:nth-child(4):not([id])",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > a:nth-child(3):not([id])",
		"body:not([id]) > div#cookieConsentBanner > div#cookieConsentContent > button:nth-child(4)#closeConsentBanner",
		"body:not([id]) > div#cookies-modal > div:nth-child(2)#consent-modal > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(1):not([id]) > span#consent-modal-refuse",
		"body > div:not([id]) > a:nth-child(3):not([id])",
		"body > div#root > div:nth-child(2):not([id]) > div:nth-child(5):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div#tna-cookie-prompt-banner > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2)#reject-cookie",
		"body > div#ecom2-spa-root > div:nth-child(4)#layout-grid > div:nth-child(4)#main-content-v2 > div:nth-child(2):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])[data-testid=\"cookie-btn-deny\"]",
		"body > div:not([id]) > div:not([id]) > button:nth-child(5):not([id])",
		"body > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > a:not([id])",
		"body > div#gdprCookieBar > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(3):not([id])",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body:not([id]) > div#c-footerBrandV1 > footer:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div#ez-cookie-notification > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(1)#ez-cookie-notification__decline",
		"body > div#site-wrapper > div:nth-child(1)#site-canvas > div#wrapper1 > main:nth-child(2)#mainSection1 > div:nth-child(3):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2)#cookieadmin_reject_button",
		"body > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:not([id])",
		"body > div#privacy-cookie-banners-root > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id])[data-testid=\"CookieBanner\"] > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(1):not([id])",
		"body > div:not([id]) > form:nth-child(1):not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div#react-application > div:not([id]) > div:not([id])[data-testid=\"linaria-injector\"] > div:nth-child(1):not([id]) > div:not([id]) > div:nth-child(2):not([id])[data-testid=\"main-cookies-banner-container\"] > section:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:not([id])",
		"body > div:not([id]) > div:nth-child(2):not([id]) > div#oax-dialog-main > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > aside#cmp-banner > form:not([id]) > div:nth-child(4):not([id]) > section:not([id]) > div:not([id]) > button:nth-child(1)#cmp-deny-all",
		"body > main#main > div:nth-child(6):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(1):not([id]) > a:not([id])",
		"body > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(1):not([id]) > button:nth-child(2):not([id])[data-testid=\"s-r-bu\"]",
		"body > div#consent_manager-background > div:nth-child(1)#consent_manager-wrapper > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2)#consent_manager-accept-none",
		"body > div#ampsandConsentElement > div:nth-child(1):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(3):not([id])",
		"body > section#cookie-consent > div:not([id]) > form:nth-child(3):not([id]) > div:nth-child(5):not([id]) > button:nth-child(1):not([id])",
		"body > div:not([id]) > div#cookie-banner > div:nth-child(2):not([id]) > button:nth-child(2)#cookie-banner-reject",
		"body > div:not([id])[data-popup=\"\"][data-popup-cookies=\"\"][data-terms-cookies-popup-common=\"\"][data-popup-need-overlay=\"true\"] > div:not([id])[data-terms-cookies-popup=\"\"][data-terms-cookies-popup-common=\"\"] > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])[data-cookies=\"disallow_all_cookies\"]",
		"body > privacy-banner:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id]) > span:not([id])",
		"body > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(1)#hw-cc-notice-deny-btn",
		"body > div#cookieconsent > div#cookieconsent-bar > div:nth-child(3):not([id]) > a:nth-child(2)#decline",
		"body > header#header > div:nth-child(1)#CookiesConsent > div:not([id]) > div:nth-child(2):not([id]) > form:nth-child(2):not([id]) > div:nth-child(5):not([id]) > button:not([id])[data-testid=\"AcceptRequiredCookies\"]",
		"body > div:not([id]) > div:nth-child(1)#ccm-widget > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div#cookieChoiceInfo > div:nth-child(2)#cookieButtonBar > a:nth-child(2)#cookieChoiceRefuse",
		"body > dialog:not([id]) > div:nth-child(1):not([id]) > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button:not([id])",
		"body > dialog:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(1):not([id]) > form:not([id]) > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button:not([id])",
		"body > div#consent-manager > div:nth-child(1)#consent-banner > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button#consent-banner-btn-close",
		"body > div#orejime > div:nth-child(1):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > ul:nth-child(2):not([id]) > li:nth-child(2):not([id]) > button:not([id])",
		"body > section#cookie-policy > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div#cbgccp-cookies-banner > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(2)#cbg_ccp_cookie_refuse_optional_btn",
		"body > div > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(5):not([id]) > button:nth-child(1)#fd-unCheckAll",
		"body > div:not([id]) > div#csm-wrapper > div:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(2):not([id])",
		"body > div#__next > div:nth-child(5):not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div#__next > div:nth-child(5):not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(1):not([id])",
		"body > dialog:not([id]) > article:not([id]) > footer:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#entry > div:nth-child(2)#main > div:nth-child(4):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div#__next > div:nth-child(3):not([id]) > dialog:nth-child(2):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#cb-cookie-warning > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > button:nth-child(1)#cb-cookie-warning__button--decline",
		"body > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div#cookies-banner > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(3):not([id]) > div:nth-child(2)#gdpr-popup-v3-button-mandatory",
		"body > div:not([id]) > div:nth-child(1):not([id])[data-testid=\"privacy-banner\"] > div:nth-child(2):not([id]) > div:nth-child(3):not([id]) > button:nth-child(2)#react-aria491930105-\\:r29\\:[data-testid=\"privacy-banner-decline-all-btn-desktop\"]",
		"body > div#tc-privacy-wrapper > div#footer_tc_privacy > div:not([id]) > div:nth-child(2)#footer_tc_privacy_container_button > button:nth-child(2)#footer_tc_privacy_button_2",
		"body > div#tc-privacy-wrapper > div:nth-child(2)#footer_tc_privacy > div:nth-child(2)#footer_tc_privacy_container_button > button:nth-child(3)#footer_tc_privacy_button_3",
		"body > div#app > div:nth-child(1)#__nuxt > div#__layout > div:not([id]) > div:nth-child(4):not([id]) > div:not([id]) > div:nth-child(4):not([id]) > a:nth-child(2):not([id])",
		"body > footer:not([id]) > div:nth-child(2):not([id]) > cookie-notice:nth-child(1):not([id]) > div#cookie-notice > div:not([id]) > div#cookie-notice-inner > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(1):not([id]) > button#cookie-notice-decline",
		"body > div#sell-root > div:nth-child(2)#appRoot > div:nth-child(7):not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div:not([id]) > div:nth-child(2)#BorlabsCookieBox > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > p:nth-child(6):not([id]) > a:not([id])",
		"body > div:not([id]) > div:nth-child(1)#consent-manager > footer:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#app > div:nth-child(7):not([id])[data-testid=\"cookieBanner\"] > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])[data-testid=\"onlyNecessaryCookies\"]",
		"body > div:not([id]) > dialog:nth-child(2):not([id])[data-testid=\"modal-dialog\"] > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(4):not([id]) > button:nth-child(2):not([id])[data-testid=\"uc-button-decline\"]",
		"body > div#tc-privacy-wrapper > div#footer_tc_privacy > div:nth-child(2)#footer_tc_privacy_container_button > button:nth-child(2)#footer_tc_privacy_button_2",
		"body > div#root > div:nth-child(2):not([id]) > div:not([id]) > div:not([id])[data-testid=\"cookie-consent-banner\"] > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div#c-pop > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(3)#ctl07_declinebtn",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(3):not([id])",
		"body > div#cookieNoticePro > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2)#cookieReject",
		"body > div#gdrp-cookieoverlay > div:nth-child(3)#cs2gdpr-cookiebanner > div:not([id]) > div:nth-child(3):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button#btn-accept-required-banner",
		"body > div#seeGdprCookieConsent > div:not([id]) > p:nth-child(2):not([id]) > a:nth-child(2)#seeGdprReject",
		"body > div#tc-privacy-wrapper > div#popin_tc_privacy > div:nth-child(2)#popin_tc_privacy_container_button > button:nth-child(2)#popin_tc_privacy_button_2",
		"body > div#SgCookieOptin > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#__next > div:nth-child(4):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(1):not([id])",
		"body > div#cookieman-modal > div:not([id]) > div:not([id]) > div:not([id]) > button:nth-child(6):not([id])",
		"body > div#c20343 > div:nth-child(1)#ikanos-privacy-cookielayer > div:nth-child(2):not([id]) > form:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(2):not([id])",
		"body > div#consent-manager > div:nth-child(1)#consent-banner > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button#consent-banner-btn-close",
		"body > div#mm-2 > div:nth-child(2)#cookie-consent > div:not([id]) > form:nth-child(3):not([id]) > div:nth-child(5):not([id]) > button:nth-child(2):not([id])",
		"body > div#root > div:not([id]) > div:nth-child(2):not([id]) > main:nth-child(2):not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(1):not([id])",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(1):not([id]) > button:nth-child(1):not([id])",
		"body > div#application > div:nth-child(4):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > a:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(6):not([id]) > div:nth-child(2)#BorlabsCookieBox > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > p:nth-child(5):not([id]) > a:not([id])",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(3):not([id]) > a:not([id])",
		"body > dialog#ai-hinweis > div:nth-child(1):not([id]) > div:nth-child(3):not([id]) > div:nth-child(3):not([id]) > center:not([id]) > button:nth-child(1)#policy_notwendig",
		"body > div:not([id]) > div:nth-child(1)#ccm-widget > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(3):not([id])",
		"body > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > a:nth-child(3):not([id])",
		"body > div#BannerRegion > div:nth-child(1)#Banner_cookie_0 > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(2)#rejectAllBtn",
		"body > comply-consent-manager:not([id]) > div#comply-consent-manager > div:nth-child(1):not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div#__next > div:nth-child(3):not([id]) > div:nth-child(3):not([id]) > div:nth-child(3):not([id])[data-testid=\"cookie_notice_reject_all_button\"]",
		"body > div#tc-privacy-wrapper > div:nth-child(2)#popin_tc_privacy > div:nth-child(2)#popin_tc_privacy_container_button > button:nth-child(2)#popin_tc_privacy_button_2",
		"body > div#__nuxt > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > section:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(1):not([id])",
		"body > div:not([id]) > div:nth-child(2):not([id]) > div#cdk-overlay-0 > mat-dialog-container:nth-child(2)#mat-mdc-dialog-0 > div:not([id]) > div:not([id]) > hra-consent-layer-ui:not([id]) > hra-cookie-buttons:nth-child(2):not([id]) > div:not([id]) > button:nth-child(1):not([id]) > span:nth-child(2):not([id])",
		"body > div#cookie-note-main > div:nth-child(2):not([id]) > button:nth-child(2)#cookie-accept-required",
		"body > aside:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:not([id]) > div:nth-child(5):not([id]) > button:nth-child(4):not([id])",
		"body > div#cookiebanner-body > div:nth-child(1)#cookiebanner > div:nth-child(2):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > a:not([id])",
		"body > div#app > div:nth-child(1)#header > div:nth-child(2)#cookie_banner > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div#__nuxt > div#__layout > div#app > div:nth-child(6):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(1):not([id])",
		"body > div#__nuxt > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > section:nth-child(1):not([id]) > div:nth-child(3):not([id]) > div:nth-child(1):not([id]) > div:not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(4):not([id]) > div:nth-child(1):not([id]) > div:not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > section#shopify-pc__banner > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(3)#shopify-pc__banner__btn-decline",
		"body > div#__nuxt > div#__layout > div:not([id]) > section:not([id]) > div:nth-child(2):not([id]) > section:nth-child(2):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#consent-manager > div:nth-child(1)#consent-banner > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button#consent-banner-btn-close",
		"body > div:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > p:nth-child(2):not([id]) > a:not([id])",
		"body > div#tc-privacy-wrapper > div:nth-child(2)#popin_tc_privacy > div:nth-child(2)#popin_tc_privacy_container_button > button:nth-child(3)#popin_tc_privacy_button_3",
		"body > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(3):not([id]) > div:nth-child(1):not([id]) > button:nth-child(1):not([id])",
		"body > div#modal-wrap > section#modal-content > div:not([id]) > div:nth-child(2)#cookie-banner > div:not([id]) > div:nth-child(1)#s1 > p:nth-child(5):not([id]) > a:nth-child(4)#button_reject",
		"body > div#cookie-consent-banner > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button:nth-child(3)#btn-reject-all",
		"body > div#cc-button > div#cc-text > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div#page > main:nth-child(2)#index > div:nth-child(3)#cookie-settings-modal > div:not([id]) > div#cookie-settings > div:nth-child(4):not([id]) > button:nth-child(3):not([id])",
		"body > div#__nuxt > div:nth-child(8)#cookie-banner > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div#consentBanner > div:not([id]) > div#gdpr-banner-container[data-testid=\"gdpr-banner-container\"] > dialog#gdpr-banner > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(3)#gdpr-banner-cmp-button[data-testid=\"gdpr-banner-decline-button\"]",
		"body > div#consentWidget > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2)#denyBtn",
		"body > div#portal-footer > div:nth-child(3):not([id]) > div:nth-child(2)#footer-analytics > div#CookieConsent > div:not([id]) > p:nth-child(2):not([id]) > span:nth-child(3):not([id]) > button:not([id])",
		"body > div#cookieman-modal > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:not([id]) > button:nth-child(1):not([id])",
		"#tc-privacy-wrapper",
		"[data-testid=\"consentManager\"]",
		"[data-testid=\"privacyBanner-rejectAll\"]",
		"body > div#ww_bzga_matomo_cookiebanner > div:not([id]) > p:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div#ddt-M1 > div:not([id]) > div:nth-child(1)#ddt-Seite1 > div:nth-child(2)#ddt-sectionFirst > p:nth-child(4):not([id]) > a:nth-child(2):not([id])",
		"body > div:not([id]) > div#cookies-bar > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div#csm-wrapper > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(2):not([id])",
		"body > div#cookie-consent > div:not([id]) > form:nth-child(3):not([id]) > div:nth-child(5):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(2)#cookie-consent[data-testid=\"cookie-consent-banner\"] > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div#tc-privacy-wrapper > div#popin_tc_privacy > div:nth-child(2)#popin_tc_privacy_container_button > button:nth-child(3)#popin_tc_privacy_button_3",
		"body > div#_evidon-barrier-wrapper > div:nth-child(2)#_evidon-banner > div:nth-child(1)#_evidon-banner-content > div:nth-child(5):not([id]) > button:nth-child(2)#_evidon-barrier-declinebutton",
		"div:not([id]) > div:not([id]) > div:nth-child(4):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(2)#BorlabsCookieBox > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > p:nth-child(2):not([id]) > a:not([id])",
		"body > div#consent_modal[data-testid=\"Over18ModalVariant1Modal\"] > div:nth-child(2):not([id])[data-testid=\"Over18ModalVariant1Content\"] > div:nth-child(3):not([id]) > div:nth-child(1):not([id]) > button:nth-child(1)#reject-all-cookies-btn",
		"body > div:not([id]) > div:nth-child(1)#matomoCookieNotification > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button#disallowMatomoCookieNotification",
		"body > div:not([id]) > aside:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button:not([id])",
		"body > div#et-consent-overlay > div:nth-child(5):not([id]) > div:nth-child(1):not([id]) > button:nth-child(1):not([id])",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > span:nth-child(1):not([id]) > button:not([id])",
		"body > div#__nuxt > div:not([id]) > div:nth-child(6):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(1):not([id])",
		"body > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > a:nth-child(1):not([id])",
		"body > div:not([id]) > div:nth-child(1)#cookies-eu-banner > div:not([id]) > div:not([id]) > button:nth-child(4)#cookies-eu-reject",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > ul:not([id]) > li:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#__nuxt > div:not([id]) > div:nth-child(8):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(1):not([id])",
		"body > div#optInId > div:nth-child(4):not([id]) > button:nth-child(2)#Ablehnen",
		"body > div:not([id]) > div#cpnb > div#w357_cpnb_outer > div:not([id]) > div:nth-child(2):not([id]) > span:nth-child(2)#cpnb-decline-btn",
		"body > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div#a-page > div:nth-child(5)#sc-content-container > div:nth-child(1):not([id]) > div:nth-child(12):not([id]) > div:nth-child(1) > div:nth-child(2)#cookie-consent-window > div:not([id]) > div:nth-child(2) > div:nth-child(1)#cookie-consent-continue > div:not([id]) > a:not([id])",
		"body > div:not([id]) > div:nth-child(1)#ccm-widget > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#__next > div:not([id]) > div:not([id]) > main:nth-child(2):not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(4):not([id]) > div:nth-child(1):not([id])",
		"body > div#acris--page-wrap--cookie-permission > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2)#cookie-permission--accept-only-functional-button",
		"body > div#cookieConsentBanner > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(1)#consentMinimal",
		"body > div#elGuestTerms > div:not([id]) > div:nth-child(2):not([id]) > form:not([id]) > button:nth-child(3):not([id])",
		"body > div#mm-0 > div:nth-child(2)#cookie-consent > div:not([id]) > form:nth-child(3):not([id]) > div:nth-child(5):not([id]) > button:nth-child(1):not([id])",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id])",
		"body > div#cookiebar > div:not([id]) > span:nth-child(3)#declineCookie > button:not([id])",
		"body > main#content > aside:nth-child(10):not([id]) > div:not([id]) > wm-stack:nth-child(2):not([id]) > wm-button:nth-child(1):not([id]) > button:not([id])",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(1):not([id]) > button:not([id])",
		"body > div#tc-privacy-wrapper > div#footer_tc_privacy > div:nth-child(1)#footer_tc_privacy_container_text > div#footer_tc_privacy_text > h2:nth-child(1):not([id]) > button#footer_tc_privacy_button_2",
		"body > div#bandeau_cgv > div:not([id]) > div:not([id]) > a:nth-child(1)#dismiss-cookies",
		"body > div:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(1):not([id])",
		"body > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(1):not([id]) > div:nth-child(3):not([id]) > button:nth-child(1):not([id])",
		"body > div:not([id]) > section:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(1):not([id])",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > ul:nth-child(2):not([id]) > li:nth-child(2):not([id]) > button:not([id])",
		"body > article:not([id]) > header:nth-child(2)#header > div:nth-child(1)#cookie-modal > div:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > button:nth-child(2):not([id])",
		"body > div#root > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > div:nth-child(1):not([id]) > button:not([id])",
		"body > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2)#cookieRefuse",
		"body > div#cookie-banner > div:nth-child(2):not([id]) > div:nth-child(3):not([id]) > button:nth-child(3)#reject-cookies",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div#P0-0 > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > ul:nth-child(2):not([id]) > li:nth-child(2):not([id]) > button:not([id])",
		"body > div#ppms_cm_consent_popup_7a8e4f13-52de-496c-9239-96129f02cf08 > div#ppms_cm_popup_overlay > div#ppms_cm_popup_wrapper > div:nth-child(1)#ppms_cm_popup > div:nth-child(3)#ppms_cm_popup_main_id > div:nth-child(2)#ppms-124c44a9-52c6-4136-9964-9a6be955271a > button:nth-child(2)#ppms_cm_disagree",
		"body > div#cookie > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > a:nth-child(2):not([id]) > span:not([id]) > span:not([id])",
		"body > div#orejime > div:nth-child(2):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > ul:nth-child(2):not([id]) > li:nth-child(2):not([id]) > button:not([id])",
		"body > div#cookie-message > a:nth-child(2):not([id])",
		"body > div#tc-privacy-wrapper > div#popin_tc_privacy > div:nth-child(1)#popin_tc_privacy_container_text > div#popin_tc_privacy_text > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2)#refuse_all",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#main > div:nth-child(11)#cookbar_overlay > div#cookbar > span:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > a:nth-child(3):not([id])",
		"body > div#tc-privacy-wrapper > div#popin_tc_privacy > div:nth-child(2)#popin_tc_privacy_container_button > button:nth-child(1)#popin_tc_privacy_button",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > main#main > dialog:nth-child(25):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(3):not([id])",
		"body > div#cookie-banner > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(1):not([id]) > button:not([id])",
		"body > div:not([id]) > div:nth-child(1):not([id])[data-testid=\"privacy-banner\"] > div:nth-child(2):not([id]) > div:nth-child(3):not([id]) > button:nth-child(2)#react-aria526766221-\\:r0\\:[data-testid=\"privacy-banner-decline-all-btn-desktop\"]",
		"body > div:not([id]) > form:nth-child(4):not([id]) > div:nth-child(2):not([id]) > div:nth-child(1):not([id]) > button#cookie_consent_use_only_functional_cookies",
		"body > div#cookiesplus-modal-container > div:not([id]) > div:nth-child(1)#cookiesplus-modal > div:nth-child(3)#cookiesplus-content > div:not([id]) > form#cookiesplus-form > div:nth-child(3):not([id]) > div:nth-child(1):not([id]) > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > button:not([id])",
		"body > app-root:not([id]) > div:not([id]) > app-cookie-banner:nth-child(3):not([id]) > div:nth-child(1):not([id]) > div:nth-child(2)#reject-cookie-link > button#reject-cookie",
		"body > main:not([id]) > footer:nth-child(4)#footer > div:nth-child(3):not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(3)#acb-banner > div:nth-child(2)#acb-action > button:nth-child(1)#acb-deny-all-button",
		"body > div#st-cmp-v2 > div:not([id]) > div:nth-child(3):not([id]) > div:not([id]) > div:nth-child(4):not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > span:not([id]) > div:not([id])",
		"body > div#cookie-consent > div:nth-child(1):not([id]) > button:nth-child(1)#cookie-deny-button",
		"body > div#advencyRgpd > button:nth-child(3)#refuseAll",
		"body > div#rgpd > div:nth-child(1):not([id]) > div:not([id]) > button:nth-child(1):not([id])",
		"body > div#consent > user-consent:nth-child(1):not([id]) > consent-dialog:not([id]) > consent-content:not([id]) > consent-message:nth-child(1):not([id]) > button:nth-child(1):not([id])",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > a:nth-child(2):not([id])",
		"body > div#bottom-banner > div:nth-child(3):not([id]) > div:nth-child(1)#cookies-win > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:not([id])[data-testid=\"cookie-consent-banner\"] > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div:not([id]) > div:nth-child(2)#BorlabsCookieBox > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > p:nth-child(8):not([id]) > a:not([id])",
		"body > div#___gatsby > div:nth-child(1)#gatsby-focus-wrapper > div:nth-child(3):not([id]) > div:nth-child(3):not([id]) > button:nth-child(1):not([id])",
		"body > div#privacy-banner > div:nth-child(3):not([id]) > button:nth-child(1)#privacy-rejected",
		"body > div#cookie > div:not([id]) > div:nth-child(2)#Buttondivanalytic > button:nth-child(2):not([id])",
		"body > div:not([id]) > p:nth-child(2):not([id]) > button:nth-child(2)#seopress-user-consent-close",
		"body#top > div:not([id]) > form:nth-child(1):not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div#cookie-notice > div:not([id]) > span:nth-child(2)#cn-notice-buttons > button:nth-child(2)#cn-refuse-cookie",
		"body:not([id]) > div#BannerRegion > div:nth-child(1)#Banner_cookie_0 > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(2)#rejectAllBtn",
		"body:not([id]) > div#tc-privacy-wrapper > div#footer_tc_privacy > div:nth-child(2)#footer_tc_privacy_container_button > button:nth-child(2)#footer_tc_privacy_button_2",
		"body#html-body > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body:not([id]) > div#modal-root > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(1):not([id]) > button:nth-child(2):not([id]) > div:not([id])",
		"body:not([id]) > div#root > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#cookie-banner-container > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div#cookie-law > div:not([id]) > div:nth-child(1):not([id]) > button:nth-child(12):not([id])",
		"body:not([id]) > div#__next > div:not([id]) > div:nth-child(8):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body:not([id]) > div#root > div:nth-child(3):not([id]) > div:nth-child(3):not([id]) > button:nth-child(1):not([id])",
		"body:not([id]) > header#header > div:nth-child(1)#CookiesConsent > div:not([id]) > div:nth-child(2):not([id]) > form:nth-child(2):not([id]) > div:nth-child(5):not([id]) > button:not([id])",
		"body:not([id]) > div#__next > div:not([id]) > div:nth-child(6)#cw-footer-container > footer:nth-child(1):not([id]) > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > div:nth-child(1):not([id]) > button:not([id]) > span:nth-child(2):not([id]) > span:nth-child(1):not([id])",
		"body > div#cookie_alert > div:nth-child(2)#cookie_alert_container > div#cookie_alert_text > div:nth-child(2):not([id]) > div:nth-child(2)#js-cookie_alert_button_decline > a#cookie_alert_decline",
		"body > div > div:not([id]) > div:nth-child(1) > div:nth-child(2)#cookieDisclaimer > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(3):not([id]) > button:not([id])",
		"body:not([id]) > dialog:not([id]) > div:nth-child(1):not([id]) > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button:not([id])",
		"body:not([id]) > dialog:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(4):not([id]) > div:nth-child(2):not([id]) > button:not([id])",
		"body#unlogged-body > div#cookie-banner-deezer > div#gdpr-dir-tag > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button#gdpr-btn-refuse-all",
		"body:not([id]) > section:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body:not([id]) > section#cookie-policy > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body:not([id]) > div#cc--main > div#cc_div > div:nth-child(1)#cm > div#c-inr > div:nth-child(2)#c-bns > button:nth-child(1)#c-s-bn",
		"body > div#headlessui-portal-root > div:not([id]) > div:nth-child(2):not([id]) > div#headlessui-dialog-_r_0_ > div:nth-child(2):not([id]) > div#headlessui-dialog-panel-_r_6_ > div:nth-child(3):not([id]) > div:not([id]) > div:nth-child(1):not([id]) > button:nth-child(1):not([id])",
		"body > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > a:nth-child(3):not([id])",
		"body:not([id]) > div#cookie-wall > div#cwi > div:nth-child(4)#cw-controls > div:not([id]) > div:nth-child(4):not([id]) > button:nth-child(2)#cwc-reject > span:not([id])",
		"body > div:not([id]) > div#csm-wrapper > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#sliding-popup > div#cookiepopup > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body:not([id]) > div#elGuestTerms > div:not([id]) > div:nth-child(2):not([id]) > form:not([id]) > button:nth-child(3):not([id])",
		"body:not([id]) > div#consent-box > p:not([id]) > span:not([id]) > button:nth-child(2)#decline-cookies",
		"body > div#__next > div:nth-child(3)#cookieBanner > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > form:nth-child(2):not([id]) > button:nth-child(2)#cookieConsentRejectAll",
		"body:not([id]) > section:not([id]).privacy-banner > div:not([id]).privacy-banner__wrap > div:not([id]).privacy-content > div:not([id]).privacy-banner__grid-wrap > div:not([id]).privacy-banner__grid-col.privacy-banner__inner > div:nth-child(3):not([id]).privacy-banner__actions.privacy-banner__set.privacy-banner__btn-wrapper--stacked > button:nth-child(2):not([id]).privacy-banner__btn.privacy-banner__btn--secondary.privacy-banner__reject",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button#confirmSelection",
		"body:not([id]) > div:not([id]) > div:nth-child(5):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(2):not([id])",
		"body:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(3):not([id]) > div:nth-child(2)#gdpr-popup-v3-button-mandatory",
		"body#hgcBody > div#cookies-banner > a:nth-child(6)#btnCookieBannerNo",
		"body:not([id]) > dialog#biccy-banner > div#biccy-prompt > div:nth-child(2):not([id]) > button:nth-child(2)#biccy-reject-button",
		"body:not([id]) > div:not([id]) > div:nth-child(4):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div > div:nth-child(1) > div:nth-child(3) > div:nth-child(2)",
		"body > div > div:nth-child(1) > div:nth-child(1) > div:nth-child(2) > button:nth-child(3)",
		"body > aside#cookies-policy > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > form:nth-child(1):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(3):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body:not([id]) > div#cookies > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button#cookiesReject",
		"body > div:not([id]) > aside:nth-child(2):not([id]) > div:nth-child(2):not([id]) > div:nth-child(2)#modal-content-17 > div#pr-cookie-notice > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2)#btn-cookie-decline",
		"body > div:not([id]) > div:nth-child(14):not([id]) > div#m-cookienotice > div:not([id]) > div:nth-child(3)#action-custom-css > a:nth-child(2):not([id])",
		"body > div#__nuxt > div#__layout > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body:not([id]) > div#dux-privacy > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > div:nth-child(1):not([id]) > button:nth-child(1):not([id])",
		"body:not([id]) > div#__nuxt > div#__layout > div:not([id]) > div:nth-child(4):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"[data-testid='reject-button']",
		"body > div > div:nth-child(10) > div:nth-child(2) > div > div:nth-child(2) > div:nth-child(2)",
		"body > div#shopui-cookie-popup-container > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > a:not([id])",
		"body > section:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body:not([id]) > div#__nuxt > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body:not([id]) > div#cookie_consent_wrapper > div:not([id]) > div:nth-child(4):not([id]) > button:nth-child(2)#consent_accept_essential",
		"body > div#cookies_overlay > div#cookies_banner > div:nth-child(2)#cookies_decision > button:nth-child(2)#cookies_read_declined",
		"body:not([id]) > div#__next > div:nth-child(3):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(4):not([id]) > div:nth-child(2):not([id]) > button:not([id])",
		"body > div#cookie-consent-popup > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2)#cookie-reject",
		"body > div#cookie-consent-banner > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2)#btn-reject-all",
		"body > div:not([id]) > div:nth-child(78)#cookie-banner > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#cribbon > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(2)#select-only-necessary > span:not([id])",
		"body > div#app > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div#cookieconsent > div:nth-child(2):not([id]) > div:nth-child(3):not([id]) > div:not([id]) > button:nth-child(1)#cookie-deny",
		"body > dialog#r42CookieBar > section:nth-child(2):not([id]) > div:nth-child(3):not([id]) > button:nth-child(1):not([id])",
		"body > div#modal-cookiesettings > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(3):not([id]) > div:not([id]) > button:nth-child(3):not([id])",
		"body > div:not([id]) > div:nth-child(1)#react-content > div:nth-child(1):not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(1):not([id]) > button:nth-child(2):not([id]) > span:not([id])",
		"body > div#cookie-bar-2019 > div:nth-child(4):not([id]) > a#declineLink",
		"body > div#__next > div:nth-child(2)#cookieBanner > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])[data-testid=\"refuse-all-cookies\"]",
		"body > aside#cookie-consent-banner > div:not([id]) > form:not([id]) > div:nth-child(2):not([id]) > fieldset:nth-child(3):not([id]) > ul:not([id]) > li:nth-child(1):not([id]) > button:not([id])",
		"body > dialog:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > footer:nth-child(3):not([id]) > div:nth-child(3):not([id]) > span:not([id])",
		"body > div#cookie-popup > div:nth-child(2):not([id]) > button:nth-child(2)#reject-cookies",
		"body > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"div[data-test-consent-banner-popup] button[aria-label=\"Niet accepteren\"]",
		"body > dialog:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > p:nth-child(2):not([id]) > a:nth-child(1):not([id])",
		"body > div:not([id]) > div:nth-child(1)#react-content > div:nth-child(8):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(1):not([id]) > button:nth-child(2):not([id])",
		"body > div#consent-modal > div:not([id]) > div:nth-child(1)#consent-intro > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button#consent-deny-all",
		"body > div:not([id]) > div:nth-child(5):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div#cookielaw_banner > div:nth-child(2):not([id]) > a:nth-child(2)#cookielaw_reject",
		"body > div#__next > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(8):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > mini-profiler:not([id]) > div:nth-child(1)#nlportal > div:nth-child(2)#nlportal-cookie-consent > div:not([id]) > div:nth-child(1):not([id]) > section:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div#_app > div:nth-child(3):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#radix-_r_0_ > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(3):not([id])",
		"body > div#nimbu-consent > div:not([id]) > div:not([id]) > div:not([id]) > p:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div#container > section:nth-child(1):not([id]) > div:not([id]) > div#block-mumc-info-cookiesui > div#cookiesjsr > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > web-overlay:not([id]) > web-cookie-consent:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#wpca-lay-out-wrapper > div:nth-child(1)#wpca-bar > div:nth-child(2)#wpca-bar-meta > button:nth-child(3):not([id])",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(1)#cookiebanner-decline[data-testid=\"button-cookie-decline\"]",
		"body > div#cookiebanner > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > span:nth-child(2):not([id]) > a:nth-child(2)#cookie-niet-akkoord",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > section#cookie-consent > div:nth-child(7):not([id]) > div:nth-child(1):not([id]) > button:nth-child(1)#button-«r0»",
		"body > aside#cookies-policy > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:nth-child(3):not([id]) > form:nth-child(1):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(4):not([id]) > div:not([id]) > a:nth-child(4):not([id])",
		"body > div#__nuxt > div:not([id]) > div:nth-child(7)#modals > div:not([id]) > dialog:not([id]) > div:nth-child(3):not([id]) > div:not([id]) > div:not([id]) > button:nth-child(2):not([id])",
		"body > div#cookies-eu-wrapper > div#cookies-eu-banner > div:nth-child(2)#cookies-eu-buttons > button:nth-child(1)#cookies-eu-reject",
		"body > div#wrapper > footer:nth-child(3):not([id]) > div#footer > div:not([id]) > div:nth-child(3):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(4):not([id]) > div:nth-child(1):not([id]) > button:nth-child(2):not([id])",
		"body > div#cc-card > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > a:nth-child(2)#cc-dismiss-btn",
		"body > div#__nuxt > div:not([id]) > div:nth-child(3):not([id]) > section#modal > div:nth-child(2):not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div#cookie-bar > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:not([id])",
		"body > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id]) > span:not([id])",
		"body > div#CNID_4eb59999-7a35-450d-a08f-1661b81e5bc4 > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2)#refuseAll",
		"body > div#__nuxt > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(8)#modals > div:not([id]) > dialog:not([id]) > div:nth-child(3):not([id]) > div:not([id]) > div:not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(3)#cookie-consent[data-testid=\"cookie-consent-banner\"] > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div#_rootSiteLayout > aside:nth-child(5):not([id]) > div:nth-child(3):not([id]) > aside:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(3):not([id])",
		"body > footer:not([id]) > div:nth-child(2)#cookieParent > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(1)#noCookies",
		"body > div#siteWrapper > div:nth-child(1):not([id]) > section:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(4):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(1):not([id]) > button:nth-child(2)#bcSubmitConsentToNone",
		"body > aside:not([id]) > div:nth-child(1):not([id]) > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button:nth-child(3):not([id])",
		"body > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(3):not([id])",
		"body > div:not([id]) > div:not([id]) > button:nth-child(4):not([id])",
		"body > shn-dialog#firstTimeVisitorCookieDialog > dialog:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > section#cc-window-overlay > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(1):not([id]) > button:nth-child(1):not([id])",
		"body > div#cookieconsent > section:not([id]) > div:not([id]) > p:nth-child(6):not([id]) > a:not([id])",
		"body > ticketswap-portal:not([id]) > ul:not([id]) > div:not([id]) > div:not([id]) > span:nth-child(2):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(2)#ez-cookie-notification > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2)#ez-cookie-notification__decline",
		"body > div#__nuxt > div:nth-child(2)#__layout > div:not([id]) > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > div:nth-child(2)#cookie-consent > div:not([id]) > dialog:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div#cookie-consent-modal-description > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(2):not([id]) > span:not([id])",
		"body > div:not([id]) > div:nth-child(3):not([id]) > div:not([id]) > section#chakra-modal-\\:rd\\: > div#chakra-modal--body-\\:rd\\: > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(3):not([id]) > form:not([id]) > button:nth-child(2)#cookie_settings_disallowed",
		"body > div#app > div:not([id]) > div:nth-child(10):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(1):not([id]) > div:nth-child(1)#cookie-banner > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(3)#cookie-banner-reject",
		"body > main#maincontent > div:nth-child(1):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#svid10_17c33ae315e94c3ae6647bf > div:nth-child(1)#svid12_3f29801717d5618f8df9bf4 > div:nth-child(2):not([id]) > div:not([id]) > button:nth-child(4)#afCookieDecline",
		"body > div#CookieConsent > dialog:nth-child(2):not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button:nth-child(1)#cc-b-custom",
		"body > aza-app:not([id]) > aza-shell:not([id]) > div:nth-child(2):not([id]) > aza-cookie-message:nth-child(1):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id]) > span:not([id])",
		"body > div#app > div:not([id]) > div:not([id]) > div:nth-child(4):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#geotargetlygeoconsent1749551372435container > div:nth-child(1):not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div#ppms_cm_consent_popup_d0973bf6-9332-4ce3-bc7c-09678b958daf > div#ppms_cm_popup_overlay > div:nth-child(2)#ppms_cm_popup_wrapper > div#ppms_cm_popup > div#ppms_cm_popup_main_id > div#ppms_cm_popup_responsive_wrapper_id > div:nth-child(2)#ppms-4ad6f5ba-7508-4caa-bd23-e8e9a4bd64b2 > div:nth-child(2)#ppms-85b001a4-b886-4f7e-82d5-bd34f50f3266 > button:nth-child(2)#ppms_cm_reject-all",
		"body > div#__nuxt > div:nth-child(2)#main-wrapper > div:nth-child(5):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#shopify-section-sections--20055816765655__privacy-banner > cookie-bar:not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > form:not([id]) > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div#cookie-consent-modal > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(4):not([id])",
		"body > div#CookieConsent > dialog:nth-child(2):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button:nth-child(1)#cc-b-custom",
		"body > div#app > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:not([id])",
		"body > div#__docusaurus > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(1)#rcc-decline-button",
		"body > div#CookieConsent > dialog:nth-child(2):not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > button#cc-b-custom",
		"body > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div#radix-_r_2_ > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div#__next > div:nth-child(3):not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > button:nth-child(1):not([id])",
		"body > section#component-cookie-banner > div:nth-child(1):not([id]) > footer:nth-child(4):not([id]) > div:nth-child(1):not([id]) > button:nth-child(2)#cookie-banner-accept-essentials",
		"body > form#cookieConsentForm > div#cookie-consent-modal > div:not([id]) > div#cookie-consent-modal-content > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div#__nuxt > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div > div:not([id]) > div:not([id]) > div:nth-child(3):not([id]) > form:nth-child(2):not([id]) > button:nth-child(3):not([id])",
		"body > div#cookie_banner > div:not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:not([id]) > div:nth-child(4):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(3):not([id]) > container:nth-child(2):not([id]) > div:not([id]) > div:not([id]) > div#versionized-cookie-banner > div:nth-child(2):not([id]) > ul:not([id]) > li:nth-child(2):not([id]) > button#btn-reject-cookies",
		"body:not([id]) > div#consent-banner > div:nth-child(2)#truste-consent-track > div:nth-child(3)#truste-consent-content > div:nth-child(3):not([id]) > div#truste-consent-buttons > button:nth-child(3)#truste-consent-required",
		"body > div#__next > div:not([id]) > div:nth-child(1)#cookieBanner[data-testid=\"cookie-banner\"] > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(1):not([id]) > p:not([id]) > a:nth-child(2):not([id])",
		"body > div[class] > div:nth-child(2) > div:nth-child(2) > button:nth-child(2)",
		"body > div#cmpbox > div:not([id]) > div:nth-child(1)#cmpboxcontent > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > a:not([id])",
		"body > div[id][cookie-banner-data-theme] > div > div > div > div:nth-child(2) > div:nth-child(2) > button",
		"body:not([id]) > div#seers-cmp-cookie-data-hol > div:nth-child(2)#SeersCMPBannerMainBar > div:nth-child(2):not([id]) > a:nth-child(2):not([id])",
		"body > div:not([id]) > div:nth-child(3)#gdpr_cookie_info_bar-wr > div:nth-child(1):not([id]) > div:nth-child(3):not([id]) > button:nth-child(2):not([id])",
		"body > div[id][class][data-role][data-controller][style] > div > div:nth-child(2) > form > button:nth-child(3)",
		"body > div[class][tabindex] > div > div > div:nth-child(2) > div:nth-child(2) > button:nth-child(2)",
		"body > div[class][id][tabindex][role][aria-labelledby][aria-modal][style] > div > div > div:nth-child(3) > div > div > p > button:nth-child(3)",
		"body > div[id] > div:nth-child(2) > div:nth-child(4) > div > div:nth-child(2) > button:nth-child(2)",
		"body > div#cassie-widget > div:nth-child(4)#cassie_cookie_module > div:nth-child(2)#cassie_pre_banner > div:nth-child(3)#cassie_pre_banner__footer > button:nth-child(2)#cassie_reject_all_pre_banner",
		"body > div:not([id]) > div:nth-child(17):not([id]) > div:not([id]) > a:nth-child(3):not([id])",
		"body:not([id]) > div#__next > div:not([id]) > main:nth-child(3):not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:nth-child(27):not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > aside:not([id]) > div:nth-child(1):not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body > div:not([id]) > div:not([id]) > div#ch2-dialog > div:nth-child(2):not([id]) > button:nth-child(1):not([id])",
		"body > div[class][role][aria-labelledby][aria-live][lang] > div:nth-child(1) > div:nth-child(3) > div:nth-child(2)",
		"body > div[id] > div > div:nth-child(1) > div > div > div > div:nth-child(2) > button:nth-child(2)",
		"body:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"body.enable-cookie-control",
		".cookie-dialog-initial",
		".cookie-dialog-initial button.g-button.outline",
		"cookie_preferences=%7B%22allow%22%3A%5B%5D",
		"*[aria-labelledby=\"consent-banner-title\"]",
		"#consent-banner-title",
		"button[data-testid=\"reject-button\"]",
		"ckns_explicit=1",
		"div.modal.cookiesModal.is-open",
		"div.cookiesModal__buttonWrapper > button[data-closecause=\"close-by-manage-cookies\"]",
		"button#js-manage-data-privacy-save-button",
		"#cookie-consent-banner .all4-cc-primary-button",
		"#cookie-consent-banner button[aria-label=\"Reject non-essential cookies and continue.\"]",
		"C4_CC=eyJ2ZXJzaW9uIjoxLCJjb25zZW50ZWQiOnRydWUs",
		"body > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		"#gdpr-cookie-message",
		"[data-testid='cookie-banner-reject-button']",
		"CMCCP=AD%3D0",
		"#cookie-disclaimer",
		"#cookiesel",
		"div[class*=\"Overlay__container\"]:has(div[class*=\"TCF2Popup\"])",
		"div[class*=\"TCF2Popup\"]",
		"[class*=\"TCF2Popup\"] a[href^=\"https://www.dailymotion.com/legal/cookiemanagement\"]",
		"button[class*=\"TCF2ContinueWithoutAcceptingButton\"]",
		"dm-euconsent-v2",
		"ngc-cookie-banner",
		"div.cookie-footer-container",
		"[data-testid=cookieBanner]",
		"[data-testid=cookieBanner] button",
		"[data-testid=cookieBanner__manageCookiesButton]",
		"[data-testid=cookieModal] input[type=radio][value=false]:not(:checked):not(:disabled)",
		"[data-testid=cookieModal__acceptButton]",
		"gdpr__",
		".cc-window.cc-visible",
		".cc-window .cc-dialog",
		".cc-window.cc-visible .cc-consent-require-only",
		"dji_consentmanager",
		"[id^=cookie-consent-banner]",
		"#cookie-consent-denied",
		"cookie-consent=denied",
		"#gdpr-banner",
		"#gdpr-banner-decline",
		".cookie-wrapper",
		".cookie-wrapper > .cookie-notice",
		"[data-test-id=cookie-notice-reject]",
		"#edp-cookies-banner",
		"#edp-cookies-banner .edp-cookies-refuse",
		"edp_cookie_agree=0",
		"#ef-ccpa",
		"#ef-button-ccpa-decline",
		"div#klaro",
		"xpath///div[contains(@class,'pointer-events-none')]//button[normalize-space()='Accept all' or normalize-space()='Tout accepter']",
		"xpath///button[normalize-space()='Customize' or normalize-space()='Personnaliser']",
		"xpath///button[normalize-space()='Decline' or normalize-space()='Refuser']",
		"escaparium_cookie_consent_decided=1",
		".cdk-overlay-container",
		".cdk-overlay-container app-esaa-cookie-component",
		".btn-cookie-refuser",
		".cck-container",
		".cck-actions-button[href=\"#refuse\"]",
		"body > div:not([id]) > div:nth-child(1):not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:nth-child(1):not([id]) > div:nth-child(2):not([id])",
		"[data-testid=\"cookie-policy-manage-dialog\"]",
		"[data-testid=\"cookie-policy-manage-dialog-decline-button\"]",
		"#gdrp",
		".cookie-consent",
		".modal-cookie",
		".cookies-modal",
		".show-cookies-modal .cookies-modal #cookies-accept",
		".show-cookies-modal .cookies-modal #cookies-reject",
		".fixed [data-testid=closeCookieBanner]",
		"[data-testid=consent-banner]",
		"[data-testid=manage-preferences]",
		"[data-testid=consent-mgr-dialog] [data-ga-button=save-preferences]",
		"div[role=\"dialog\"][aria-label^=\"Privacy Disclosure\"]",
		"#CookieConsent",
		"#CookieConsentDeclined",
		"[data-testid=consent-banner] [data-testid=reject-button]",
		"xpath///div[contains(., \"Vill du tillåta användningen av cookies från Instagram i den här webbläsaren?\") or contains(., \"Allow the use of cookies from Instagram on this browser?\") or contains(., \"Povolit v prohlížeči použití souborů cookie z Instagramu?\") or contains(., \"Dopustiti upotrebu kolačića s Instagrama na ovom pregledniku?\") or contains(., \"Разрешить использование файлов cookie от Instagram в этом браузере?\") or contains(., \"Vuoi consentire l'uso dei cookie di Instagram su questo browser?\") or contains(., \"Povoliť používanie cookies zo služby Instagram v tomto prehliadači?\") or contains(., \"Die Verwendung von Cookies durch Instagram in diesem Browser erlauben?\") or contains(., \"Sallitaanko Instagramin evästeiden käyttö tällä selaimella?\") or contains(., \"Engedélyezed az Instagram cookie-jainak használatát ebben a böngészőben?\") or contains(., \"Het gebruik van cookies van Instagram toestaan in deze browser?\") or contains(., \"Bu tarayıcıda Instagram'dan çerez kullanımına izin verilsin mi?\") or contains(., \"Permitir o uso de cookies do Instagram neste navegador?\") or contains(., \"Permiţi folosirea modulelor cookie de la Instagram în acest browser?\") or contains(., \"Autoriser l’utilisation des cookies d’Instagram sur ce navigateur ?\") or contains(., \"¿Permitir el uso de cookies de Instagram en este navegador?\") or contains(., \"Zezwolić na użycie plików cookie z Instagramu w tej przeglądarce?\") or contains(., \"Να επιτρέπεται η χρήση cookies από τo Instagram σε αυτό το πρόγραμμα περιήγησης;\") or contains(., \"Разрешавате ли използването на бисквитки от Instagram на този браузър?\") or contains(., \"Vil du tillade brugen af cookies fra Instagram i denne browser?\") or contains(., \"Vil du tillate bruk av informasjonskapsler fra Instagram i denne nettleseren?\")]",
		"xpath///button[contains(., 'Отклонить необязательные файлы cookie') or contains(., 'Decline optional cookies') or contains(., 'Refuser les cookies optionnels') or contains(., 'Hylkää valinnaiset evästeet') or contains(., 'Afvis valgfrie cookies') or contains(., 'Odmietnuť nepovinné cookies') or contains(., 'Απόρριψη προαιρετικών cookies') or contains(., 'Neka valfria cookies') or contains(., 'Optionale Cookies ablehnen') or contains(., 'Rifiuta cookie facoltativi') or contains(., 'Odbij neobavezne kolačiće') or contains(., 'Avvis valgfrie informasjonskapsler') or contains(., 'İsteğe bağlı çerezleri reddet') or contains(., 'Recusar cookies opcionais') or contains(., 'Optionele cookies afwijzen') or contains(., 'Rechazar cookies opcionales') or contains(., 'Odrzuć opcjonalne pliki cookie') or contains(., 'Отхвърляне на бисквитките по избор') or contains(., 'Odmítnout volitelné soubory cookie') or contains(., 'Refuză modulele cookie opţionale') or contains(., 'A nem kötelező cookie-k elutasítása')]",
		".pop-cookie",
		".miniConsent,#PrivacyPolicyBanner",
		"#PrivacyPolicyBanner",
		"#cookie-settings",
		"#reject-all-cookies",
		"#cookiebanner-popup",
		"#jg-chrome-header",
		"#cookiebanner-popup #accept-essential-Cookies",
		"necessary:true%2Cpreferences:false",
		"#react-consent-modal",
		"#react-consent-modal .flex.flex-column",
		"#react-consent-modal .flex.flex-row.justify-between button:first-child",
		"#gdpr-banner-container",
		"#gdpr-banner-container #gdpr-banner [data-testid=gdpr-banner-decline-button]",
		"div:has(> div > a[href*='/privacy-policy'])",
		"div:has(> div > div > div[role=alert] > a[href^=\"https://policy.medium.com/medium-privacy-policy-\"])",
		"#cookie-container",
		"div[aria-label=\"Cookie Policy Banner\"]",
		"#onetrust-banner-sdk",
		"div#cookieWarning",
		"a#btnCookiesDenyAll",
		"[class*=ConsentManager]",
		"[class*=ConsentManager_row] input[type=radio][id$=no]",
		"[class*=ConsentManager_continue]",
		"[data-testid=cookie-dialog-root]",
		"input[type=radio][id$=-declineLabel]",
		"[data-testid=confirm-choice-button]",
		"ccm-notification",
		"gdpr-banner",
		".cookies-agreement-notification,.modal-new:has([data-module=SetupCookies])",
		".cookies-agreement-notification",
		".cookies-agreement-notification .cb_setup",
		"[data-module=SetupCookies]",
		"[data-module=SetupCookies] input[type=checkbox]:checked:not(:disabled)",
		"[name=button_save_choice]",
		"div.b-cookies-informer",
		"div.b-cookies-informer__nav > button:nth-child(1)",
		"div.b-cookies-informer__switchers",
		"div.b-cookies-informer__switchers input:not([disabled])",
		"div.b-cookies-informer__nav > button",
		"[aria-labelledby=cookieConsentTitle]",
		"xpath///button[contains(., 'Reject non-essential')]",
		"consent=rejected",
		"#cookie-consent",
		"#cookie-consent .cookie-all__btn",
		"#cookie-consent .cookie-consent__switch:not(.always_on)",
		"#cookie-consent .cookie-selection__btn",
		"cookie_consent_essential=true",
		"cookie_consent_marketing=true",
		".cookie-manager",
		".cookie-manager .cookie-notice.open",
		".cookie-notice [data-test=reject]",
		"footer .ccpabanner",
		".BusinessCookieConsent",
		".BusinessCookieConsent [data-id=cookie-consent-banner-buttons]",
		"[data-id=cookie-consent-banner-buttons] > div:nth-child(2) button",
		"#cookie-consent button",
		"#cookie-consent input[type=checkbox]:checked:not(:disabled)",
		"plosCookieConsentStatus=false",
		"#cookieBanner #cookieBannerContent",
		"#cookieBanner #cookieBannerContent, #globalCookieBanner",
		"#globalCookieBanner .js-customizeGlobalCookies",
		"#cookieBanner [data-label=accept_essential]",
		"#cookieBanner button.cbSecondaryCTA",
		"pnl-cookie-wall-widget",
		"CookiePermissionInfo",
		".alert .accept-cookies,form.js-cookies",
		".alert:has(.accept-cookies)",
		"form.js-cookies button",
		"form.js-cookies input[type=checkbox]",
		"form.js-cookies input[type=checkbox]:checked:not(:disabled)",
		"form.js-cookies button[type=submit]",
		".alert:has(.accept-cookies) a[href='/account/cookies']",
		".c-modal.is-active",
		".c-modal.is-active .is-dismiss",
		"#__rptl-cookiebanner #__rptl-cookiebanner-reject",
		".DialogHandlerContainer.visible:has(.cookie-dialog)",
		"span.cookie-option-large>div>span.link-no-decoration.gui-text",
		".cookie-dialog form .CookieConsentOption input[type='checkbox']:not([name='permanent']):checked",
		".cookie-dialog button[type='submit']",
		"[bundlename=reddit_cookie_banner]",
		"#data-protection-consent-dialog rpl-modal-card > button.button-primary",
		"#data-protection-consent-dialog rpl-modal-card > button.button-secondary",
		"eu_cookie={%22opted%22:true%2C%22nonessential%22:false}",
		"div:has(> button#cookieBar-button)",
		"#cookieBar-button",
		".cookie-banner-wrapper",
		".cookie-banner-wrapper .cookie-banner",
		".cookie-banner-wrapper button.btn-secondary-lg",
		"RBXcb",
		"#cookie-policy-info",
		"div.cookie-btn-box > div[aria-label=\"Reject\"]",
		".cookie-policy-lightbox-bottom > div[aria-label=\"Save Settings\"]",
		"#m-cookienotice",
		"#manage-cookies",
		"#accept-selected",
		".cookies-banner-shown",
		"#cookie-popup-with-overlay",
		"#cookie-popup-with-overlay [data-ref='cookie.no-thanks']",
		"RY_COOKIE_CONSENT",
		"div.cookie-bar",
		"body > div#__next > div#app > div:nth-child(7):not([id])",
		"body > div[class*=_shein_privacy]",
		"body > div[class*=_shein_privacy] > div:nth-of-type(2) > div:nth-of-type(4) > div:nth-of-type(3)",
		"#cookieBannerContent",
		"[data-tracking-element-id=cookie_banner_essential_only]",
		".consent__wrapper",
		".consent",
		"button.consentSettings",
		"button#consentSubmit",
		"[class*=CookieConsent__root___]",
		"[class*=CookieConsent__modal___]",
		"[class*=CookieConsent__modal___] > div > button[class*=secondary]:nth-child(2)",
		"cookie-consent-1={\"optedIn\":true,\"functionality\":false,\"statistics\":false}",
		"div > div > div > div > span[href*=\"/cookie-and-similar-technologies-policy.html\"]",
		"xpath///span[contains(., 'Alle afwijzen') or contains(., 'Reject all') or contains(., 'Tümünü reddet') or contains(., 'Odrzuć wszystko')]",
		"div > div > div:has(> div > span[href*=\"/cookie-and-similar-technologies-policy.html\"]) > [role=button]:nth-child(2)",
		"[aria-label=consent-banner]",
		"#onetrust-pc-sdk",
		"xpath///button[contains(., 'Reject all')]",
		"interactionCount=1",
		"#cookie_banner",
		"#tsla-reject-cookie",
		"tsla-cookie-consent=rejected",
		"#consent-banner",
		"#consent-banner button[data-testid='close-button']",
		".duet--cta--cookie-banner",
		".duet--cta--cookie-banner button.tracking-12 > span",
		"_duet_gdpr_acknowledged=1",
		"[data-test-id=cookies_section]",
		"div:has(> button#accept-button):has(> button#decline-button)",
		"button#accept-button,button#decline-button",
		"button#decline-button",
		"#consent-modal",
		"#privacy-settings-content",
		"button[type=\"submit\"]",
		"div.one-modal__action-footer-column--secondary > a",
		".fixed.bottom-0:has([data-test=cookieBannerButton])",
		".fixed.bottom-0 [data-test=cookieBannerButton]",
		"div:has(> .consent-banner .consent-banner__content--gdpr-v2),.ReactModalPortal:has([data-a-target=consent-modal-save])",
		".consent-banner .consent-banner__content--gdpr-v2",
		"div:has(> .consent-banner .consent-banner__content--gdpr-v2)",
		"button[data-a-target=\"consent-banner-manage-preferences\"]",
		"input[type=checkbox][data-a-target=tw-checkbox]",
		"input[type=checkbox][data-a-target=tw-checkbox][checked]:not([disabled])",
		"[data-a-target=consent-modal-save]",
		".ReactModalPortal:has([data-a-target=consent-modal-save])",
		"[data-testid=\"BottomBar\"]",
		"body > div#react-root > div:not([id]) > div:not([id]) > div:nth-child(1)#layers > div:not([id]) > div:nth-child(2):not([id]) > div:not([id]) > div:not([id])[data-testid=BottomBar] > div:not([id]) > div:nth-child(2):not([id]) > button:nth-child(2):not([id])",
		".cookie-banner",
		".ucb",
		".ucb-banner",
		".ucb-banner .ucb-btn-save",
		".dip-consent,.dip-consent-container",
		".dip-consent-container",
		".dip-consent-content",
		".dip-consent-btn[tabindex=\"2\"]",
		"cookie-banner#cookie-banner-host",
		"groups=C1%3A1%2CC3%3A0",
		"[class^=cookie_wrapper]",
		"[class*=ConsentBanner]",
		"[class*=ConsentBanner] .frDWEu",
		"[class*=ConsentBanner] .hXIpFU",
		"xeConsentState={%22performance%22:false%2C%22marketing%22:false%2C%22compliance%22:false}",
		"[class*=modal]",
		"[class*=modal] a[href*='/cookie-policy']",
		"[class*=modal]:has(a[href*='/cookie-policy']) button:nth-child(2)",
		"[class*=cookiesAnnounce]",
		"[class*=cookiesAnnounce] [class*=announceText]",
		".disclaimer-opened #disclaimer-cookies",
		"#disclaimer-reject_cookies",
		"#consent-page",
		"#consent-page button[value=reject]",
		"#cookie_modal_wrapper",
		"#cookie_modal_wrapper #cookie_modal_button_choose",
		"#consent-init",
		"#consent-init #consent-configure",
		"#consent-update #consent-configuration-save",
		"zinio-cookie-consent"
	],
	r: [
		[
			1,
			"abconcerts.be",
			2,
			"",
			22,
			[0],
			[{ "e": 1 }],
			[{ "v": 1 }],
			[{
				"if": { "e": 2 },
				"then": [{ "k": 2 }],
				"else": [{ "k": 3 }, { "c": 4 }]
			}],
			[],
			{ "intermediate": false }
		],
		[
			1,
			"acris",
			2,
			"",
			22,
			[5],
			[{ "e": 6 }],
			[{ "v": 7 }],
			[
				{
					"check": "any",
					"wv": 8
				},
				{ "wait": 500 },
				{ "c": 8 }
			],
			[],
			{}
		],
		[
			1,
			"adopt",
			2,
			"",
			22,
			[9],
			[{ "e": 10 }],
			[{ "v": 9 }],
			[{ "c": 11 }],
			[{ "eval": "EVAL_ADOPT_TEST" }],
			{}
		],
		[
			1,
			"Adroll",
			2,
			"",
			22,
			[12],
			[{ "e": 12 }],
			[{ "v": 12 }],
			[{ "c": 13 }],
			[{
				"negated": true,
				"cc": 14
			}],
			{}
		],
		[
			1,
			"affinity.serif.com",
			2,
			"",
			22,
			[],
			[{ "e": 15 }],
			[{ "v": 16 }],
			[{ "k": 17 }],
			[
				{ "wait": 500 },
				{ "cc": 18 },
				{
					"negated": true,
					"cc": 19
				}
			],
			{}
		],
		[
			1,
			"amazon.com",
			2,
			"",
			22,
			[20],
			[{ "e": 20 }],
			[{
				"check": "any",
				"v": 21
			}],
			[
				{ "wv": 22 },
				{ "wait": 5e3 },
				{ "k": 22 }
			],
			[],
			{}
		],
		[
			1,
			"amex",
			0,
			"",
			22,
			[23],
			[{ "e": 23 }],
			[{ "v": 23 }],
			[{ "c": 24 }],
			[],
			{}
		],
		[
			1,
			"anthropic",
			2,
			"",
			22,
			[],
			[{ "e": 25 }],
			[{ "v": 25 }],
			[{ "c": 26 }],
			[{ "cc": 27 }],
			{}
		],
		[
			1,
			"aquasana.com",
			2,
			"",
			22,
			[28],
			[{ "e": 28 }],
			[{ "e": 28 }],
			[{
				"if": { "e": 29 },
				"then": [{ "k": 29 }],
				"else": [{ "h": 28 }]
			}],
			[],
			{}
		],
		[
			1,
			"arbeitsagentur",
			2,
			"",
			22,
			[30],
			[{ "e": 31 }],
			[{ "e": 32 }],
			[{ "waitForThenClick": ["bahf-cookie-disclaimer-dpl3", "#bahf-cookie-disclaimer-modal .ba-btn-contrast"] }],
			[{ "cc": 33 }],
			{}
		],
		[
			1,
			"asus",
			2,
			"",
			22,
			[34],
			[{ "e": 35 }],
			[{ "v": 35 }],
			[{
				"if": { "e": 36 },
				"then": [{ "k": 36 }],
				"else": [{ "c": 37 }, { "c": 38 }]
			}],
			[],
			{}
		],
		[
			1,
			"automattic-cmp-optout",
			2,
			"",
			22,
			[39],
			[{ "e": 39 }],
			[{ "v": 39 }],
			[
				{ "k": 40 },
				{
					"all": true,
					"c": 41
				},
				{ "k": 42 }
			],
			[],
			{}
		],
		[
			1,
			"aws.amazon.com",
			2,
			"",
			22,
			[
				43,
				44,
				45,
				46
			],
			[{ "e": 43 }],
			[{ "v": 43 }],
			[
				{ "k": 47 },
				{ "w": 48 },
				{
					"all": true,
					"optional": true,
					"k": 49
				},
				{ "k": 50 }
			],
			[],
			{}
		],
		[
			1,
			"axeptio",
			2,
			"",
			22,
			[51],
			[{ "e": 51 }],
			[{ "any": [
				{ "e": 52 },
				{ "v": 53 },
				{ "visible": [".axeptio_mount .needsclick", ".axeptio_widget"] }
			] }],
			[{
				"if": { "e": 54 },
				"then": [
					{ "waitForVisible": [".axeptio_mount .needsclick", "button#axeptio_btn_dismiss,button.ax-discardButton"] },
					{ "wait": 300 },
					{ "click": [".axeptio_mount .needsclick", "button#axeptio_btn_dismiss,button.ax-discardButton"] }
				],
				"else": [
					{ "wv": 55 },
					{ "wait": 300 },
					{ "k": 55 }
				]
			}],
			[{ "cc": 56 }],
			{}
		],
		[
			1,
			"b-cookie",
			1,
			"",
			22,
			[57],
			[{ "e": 57 }],
			[{ "v": 57 }],
			[{ "h": 57 }],
			[],
			{}
		],
		[
			1,
			"baden-wuerttemberg.de",
			2,
			"",
			22,
			[58],
			[{ "e": 58 }],
			[{ "v": 58 }],
			[{
				"if": { "e": 59 },
				"then": [{ "k": 59 }],
				"else": [{
					"all": true,
					"optional": true,
					"k": 60
				}, { "k": 61 }]
			}],
			[],
			{}
		],
		[
			1,
			"bbb.org",
			0,
			"",
			22,
			[62],
			[{ "e": 62 }],
			[{ "v": 63 }],
			[
				{ "wv": 64 },
				{ "wait": 500 },
				{ "k": 64 },
				{ "w": 65 },
				{
					"all": true,
					"optional": true,
					"k": 66
				},
				{ "k": 67 },
				{ "c": 68 }
			],
			[],
			{}
		],
		[
			1,
			"bigcommerce-consent-manager",
			0,
			"",
			22,
			[69],
			[{ "e": 69 }, { "eval": "EVAL_BIGCOMMERCE_CONSENT_MANAGER_DETECT" }],
			[{ "v": 70 }],
			[{ "c": 71 }],
			[{ "cc": 72 }],
			{}
		],
		[
			1,
			"bing.com",
			2,
			"",
			22,
			[73],
			[{ "e": 74 }],
			[{ "v": 74 }, { "v": 75 }],
			[{ "wait": 500 }, { "c": 76 }],
			[{ "cc": 77 }],
			{}
		],
		[
			1,
			"blocksy",
			0,
			"",
			10,
			[78],
			[{ "e": 79 }],
			[{ "v": 78 }],
			[{ "c": 80 }],
			[{ "cc": 81 }],
			{ "intermediate": false }
		],
		[
			1,
			"borlabs",
			2,
			"",
			22,
			[82],
			[{ "e": 83 }],
			[{ "v": 84 }],
			[{
				"if": { "e": 85 },
				"then": [{ "k": 85 }],
				"else": [
					{ "k": 86 },
					{ "wv": 87 },
					{
						"all": true,
						"optional": true,
						"k": 88
					},
					{ "k": 89 },
					{ "wait": 500 }
				]
			}],
			[{ "eval": "EVAL_BORLABS_0" }],
			{}
		],
		[
			1,
			"bswhealth",
			0,
			"",
			22,
			[90],
			[{ "e": 90 }],
			[{ "v": 91 }],
			[
				{ "c": 92 },
				{ "wv": 93 },
				{
					"all": true,
					"optional": true,
					"c": 94
				},
				{ "c": 91 }
			],
			[{ "cc": 95 }],
			{}
		],
		[
			1,
			"bundesregierung.de",
			2,
			"",
			22,
			[96],
			[{ "e": 96 }],
			[{ "v": 97 }],
			[{ "wait": 500 }, { "c": 98 }],
			[{ "cc": 99 }],
			{}
		],
		[
			1,
			"burpee.com",
			2,
			"",
			22,
			[100, 101],
			[{ "e": 101 }],
			[{ "v": 102 }],
			[{
				"if": { "e": 103 },
				"then": [
					{ "c": 103 },
					{
						"all": true,
						"k": 104
					},
					{ "k": 105 }
				],
				"else": [{ "h": 106 }]
			}],
			[],
			{}
		],
		[
			1,
			"cassie",
			0,
			"",
			10,
			[107],
			[{ "e": 108 }],
			[{ "v": 109 }],
			[{ "c": 110 }],
			[],
			{}
		],
		[
			1,
			"cc_banner",
			1,
			"",
			22,
			[111],
			[{ "e": 111 }],
			[{ "v": 112 }],
			[{ "h": 111 }],
			[],
			{}
		],
		[
			1,
			"cc-banner-springer",
			2,
			"",
			22,
			[113],
			[{ "e": 113 }],
			[{ "v": 113 }],
			[{
				"if": { "e": 114 },
				"then": [{ "k": 114 }],
				"else": [
					{ "c": 115 },
					{ "w": 116 },
					{
						"all": true,
						"optional": true,
						"k": 117
					},
					{
						"if": { "e": 118 },
						"then": [{ "k": 118 }],
						"else": [{ "k": 119 }]
					}
				]
			}],
			[{ "eval": "EVAL_CC_BANNER2_0" }],
			{}
		],
		[
			1,
			"check24-partnerprogramm-de",
			2,
			"",
			22,
			[120],
			[{ "e": 121 }],
			[{
				"check": "any",
				"v": 121
			}],
			[{ "c": 122 }],
			[],
			{}
		],
		[
			1,
			"ciaopeople.it",
			2,
			"",
			22,
			[123],
			[{ "e": 123 }],
			[{ "v": 123 }],
			[
				{ "c": 124 },
				{ "w": 125 },
				{ "c": 126 }
			],
			[{
				"check": "none",
				"v": 123
			}],
			{}
		],
		[
			1,
			"civic-cookie-control",
			2,
			"",
			22,
			[127],
			[{ "e": 128 }],
			[{ "v": 129 }, { "v": 128 }],
			[{
				"if": { "v": 130 },
				"then": [{ "c": 130 }],
				"else": [{
					"if": { "exists": ["#ccc #ccc-notify .ccc-notify-buttons", "xpath///button[contains(., 'Settings') or contains(., 'Cookie Preferences') or contains(., 'Einstellungen️')]"] },
					"then": [{ "waitForThenClick": ["#ccc #ccc-notify .ccc-notify-buttons", "xpath///button[contains(., 'Settings') or contains(., 'Cookie Preferences') or contains(., 'Einstellungen️')]"] }, { "wv": 131 }]
				}, {
					"if": { "v": 130 },
					"then": [{ "c": 130 }],
					"else": [{ "c": 132 }]
				}]
			}],
			[],
			{}
		],
		[
			1,
			"ckies",
			2,
			"",
			22,
			[133],
			[{ "e": 133 }],
			[{ "v": 133 }],
			[{ "c": 134 }],
			[{ "cc": 135 }],
			{}
		],
		[
			1,
			"click.io",
			2,
			"",
			22,
			[136],
			[{ "e": 136 }],
			[{ "v": 136 }],
			[
				{ "w": 137 },
				{ "wait": 500 },
				{ "k": 137 },
				{ "w": 138 },
				{
					"all": true,
					"k": 138
				},
				{ "k": 139 }
			],
			[{ "cc": 140 }],
			{}
		],
		[
			1,
			"clinch",
			2,
			"",
			10,
			[141],
			[{ "e": 141 }],
			[{ "v": 141 }],
			[{
				"if": { "e": 142 },
				"then": [{ "k": 142 }],
				"else": [
					{ "k": 143 },
					{
						"all": true,
						"optional": true,
						"k": 144
					},
					{ "k": 145 }
				]
			}],
			[{ "cc": 146 }],
			{ "intermediate": false }
		],
		[
			1,
			"cloudflare-zaraz",
			2,
			"",
			22,
			[147],
			[{ "exists": [".cf_modal_container", ".cf_consent-buttons"] }],
			[{ "visible": [".cf_modal_container", ".cf_consent-buttons"] }],
			[{ "waitForThenClick": [".cf_modal_container", ".cf_consent-buttons #cf_consent-buttons__reject-all"] }],
			[],
			{}
		],
		[
			1,
			"Complianz banner",
			2,
			"",
			22,
			[148],
			[{ "e": 149 }],
			[{
				"check": "any",
				"v": 149
			}],
			[{ "c": 150 }],
			[{ "cc": 151 }],
			{}
		],
		[
			1,
			"Complianz categories",
			2,
			"",
			22,
			[152],
			[{ "e": 152 }],
			[{ "v": 152 }],
			[{
				"if": { "e": 153 },
				"then": [{ "k": 154 }],
				"else": [{
					"all": true,
					"optional": true,
					"k": 155
				}, { "k": 156 }]
			}],
			[],
			{}
		],
		[
			1,
			"Complianz notice",
			1,
			"",
			22,
			[157],
			[{ "e": 158 }],
			[{ "v": 158 }],
			[{
				"if": { "e": 159 },
				"then": [{ "k": 159 }],
				"else": [{ "h": 160 }]
			}],
			[],
			{}
		],
		[
			1,
			"Complianz opt-both",
			2,
			"",
			22,
			[161],
			[{ "e": 161 }],
			[{ "v": 161 }],
			[{ "c": 159 }],
			[],
			{}
		],
		[
			1,
			"Complianz opt-out",
			2,
			"",
			22,
			[162],
			[{ "e": 162 }],
			[{ "v": 162 }],
			[{
				"if": { "e": 159 },
				"then": [{ "k": 159 }],
				"else": [{
					"if": { "e": 163 },
					"then": [
						{ "k": 163 },
						{ "c": 164 },
						{ "c": 165 }
					]
				}]
			}],
			[],
			{}
		],
		[
			1,
			"Complianz optin",
			2,
			"",
			22,
			[166],
			[{ "e": 166 }],
			[{ "v": 166 }],
			[{
				"if": { "v": 159 },
				"then": [{ "k": 159 }],
				"else": [{
					"if": { "v": 167 },
					"then": [
						{ "c": 167 },
						{ "wv": 168 },
						{
							"all": true,
							"optional": true,
							"k": 169
						},
						{ "k": 170 }
					],
					"else": [{ "k": 154 }]
				}]
			}],
			[],
			{}
		],
		[
			1,
			"consentmo",
			2,
			"",
			22,
			[171],
			[{ "exists": ["csm-cookie-consent", ".csm-wrapper"] }],
			[{ "visible": ["csm-cookie-consent", ".csm-wrapper button"] }],
			[{
				"if": { "exists": ["csm-cookie-consent", ".csm-wrapper .cc-deny"] },
				"then": [{ "waitForThenClick": ["csm-cookie-consent", ".csm-wrapper .cc-deny"] }],
				"else": [{ "waitForThenClick": ["csm-cookie-consent", ".csm-wrapper .cc-settings"] }, { "waitForThenClick": ["csm-cookie-consent", ".csm-wrapper .cc-deny"] }]
			}],
			[{
				"waitForVisible": ["csm-cookie-consent", ".csm-wrapper button"],
				"timeout": 1e3,
				"check": "none"
			}],
			{}
		],
		[
			1,
			"Cookie Information Banner",
			2,
			"",
			22,
			[172],
			[{ "e": 172 }],
			[{ "v": 172 }],
			[
				{ "eval": "EVAL_COOKIEINFORMATION_0" },
				{ "wait": 1e3 },
				{
					"if": { "v": 173 },
					"then": [{ "h": 172 }]
				}
			],
			[{ "cc": 174 }],
			{}
		],
		[
			1,
			"cookie-consent-spice",
			0,
			"",
			10,
			[175, 176],
			[{ "e": 176 }],
			[{ "v": 176 }],
			[{ "c": 177 }],
			[],
			{}
		],
		[
			1,
			"cookie-law-info",
			2,
			"",
			22,
			[178],
			[{ "e": 179 }, { "eval": "EVAL_COOKIE_LAW_INFO_DETECT" }],
			[{ "v": 179 }],
			[{ "h": 178 }, { "eval": "EVAL_COOKIE_LAW_INFO_0" }],
			[{
				"negated": true,
				"cc": 180
			}],
			{}
		],
		[
			1,
			"cookie-manager-popup",
			0,
			"",
			10,
			[181],
			[{ "e": 182 }],
			[{ "v": 101 }],
			[{
				"if": { "e": 183 },
				"then": [{ "k": 183 }],
				"else": [
					{ "c": 181 },
					{ "wv": 184 },
					{
						"all": true,
						"optional": true,
						"k": 185
					},
					{ "k": 186 }
				]
			}],
			[{ "eval": "EVAL_COOKIE_MANAGER_POPUP_0" }],
			{ "intermediate": false }
		],
		[
			1,
			"cookie-script",
			2,
			"",
			22,
			[187],
			[{ "e": 187 }],
			[{ "v": 187 }],
			[{
				"if": { "e": 188 },
				"then": [{ "wait": 100 }, { "k": 188 }],
				"else": [
					{ "k": 189 },
					{ "wv": 190 },
					{ "c": 188 }
				]
			}],
			[],
			{}
		],
		[
			1,
			"cookieacceptbar",
			1,
			"",
			22,
			[191],
			[{ "e": 191 }],
			[{ "v": 191 }],
			[{ "h": 191 }],
			[],
			{}
		],
		[
			1,
			"cookieconsent2",
			2,
			"",
			22,
			[192],
			[{ "e": 192 }],
			[{ "e": 193 }, { "e": 194 }],
			[{
				"if": { "e": 195 },
				"then": [{ "c": 195 }],
				"else": [
					{ "c": 196 },
					{ "wait": 500 },
					{ "c": 197 }
				]
			}],
			[{ "cc": 198 }],
			{}
		],
		[
			1,
			"cookieconsent3",
			2,
			"",
			22,
			[199],
			[{ "e": 199 }],
			[{ "v": 200 }],
			[{
				"if": { "e": 201 },
				"then": [{ "c": 201 }],
				"else": [{ "c": 202 }, { "c": 203 }]
			}],
			[{ "cc": 198 }],
			{}
		],
		[
			1,
			"cookiecuttr",
			0,
			"",
			10,
			[204],
			[{ "e": 205 }],
			[{ "v": 205 }],
			[{
				"if": { "e": 206 },
				"then": [{ "k": 206 }],
				"else": [{ "h": 204 }]
			}],
			[],
			{}
		],
		[
			1,
			"cookiefirst.com",
			2,
			"",
			22,
			[207],
			[{ "e": 208 }],
			[{ "v": 208 }],
			[{
				"if": { "e": 209 },
				"then": [
					{ "k": 209 },
					{
						"timeout": 1e3,
						"wv": 210
					},
					{ "eval": "EVAL_COOKIEFIRST_1" },
					{ "wait": 1e3 },
					{ "k": 211 }
				],
				"else": [{ "k": 212 }]
			}],
			[{ "eval": "EVAL_COOKIEFIRST_0" }],
			{}
		],
		[
			1,
			"cookiehub",
			2,
			"",
			22,
			[213],
			[{ "e": 214 }],
			[{ "v": 214 }],
			[{
				"if": { "e": 215 },
				"then": [
					{ "k": 215 },
					{ "wv": 216 },
					{
						"if": { "e": 217 },
						"then": [{ "k": 217 }],
						"else": [{
							"all": true,
							"optional": true,
							"k": 218
						}, { "k": 219 }]
					}
				],
				"else": [{ "h": 213 }]
			}],
			[{ "cc": 220 }],
			{}
		],
		[
			1,
			"cookiejs-banner",
			2,
			"",
			22,
			[221],
			[{ "e": 222 }],
			[{ "v": 223 }],
			[{ "c": 224 }],
			[{ "cc": 225 }],
			{}
		],
		[
			1,
			"cookiejs-modal",
			2,
			"",
			22,
			[221],
			[{ "e": 226 }],
			[{ "v": 227 }],
			[{ "c": 228 }],
			[{ "cc": 229 }],
			{}
		],
		[
			1,
			"cookieyes",
			2,
			"",
			22,
			[230],
			[{ "e": 231 }],
			[{ "v": 231 }],
			[{
				"if": { "e": 232 },
				"then": [{ "c": 232 }],
				"else": [{
					"if": { "e": 233 },
					"then": [
						{ "k": 233 },
						{ "w": 234 },
						{
							"all": true,
							"optional": true,
							"k": 235
						},
						{ "c": 236 }
					],
					"else": [{ "h": 237 }]
				}]
			}],
			[{ "cc": 238 }],
			{}
		],
		[
			1,
			"corona-in-zahlen.de",
			2,
			"",
			22,
			[239],
			[{ "e": 239 }],
			[{ "v": 239 }],
			[{ "k": 240 }, { "k": 241 }],
			[],
			{}
		],
		[
			1,
			"ct-ultimate-gdpr",
			2,
			"",
			22,
			[242],
			[{ "e": 242 }],
			[{
				"timeout": 3e4,
				"wv": 242
			}],
			[{
				"if": { "v": 243 },
				"then": [{ "k": 243 }],
				"else": [{
					"if": { "v": 244 },
					"then": [
						{ "k": 244 },
						{ "c": 245 },
						{ "k": 246 }
					],
					"else": [{ "h": 242 }]
				}]
			}],
			[{ "wait": 500 }, { "cc": 247 }],
			{}
		],
		[
			1,
			"curseforge",
			1,
			"",
			22,
			[248],
			[{ "e": 249 }],
			[{ "v": 249 }],
			[{ "h": 248 }],
			[],
			{}
		],
		[
			1,
			"dailymotion-us",
			1,
			"",
			22,
			[250],
			[{ "e": 251 }],
			[{ "v": 251 }],
			[{ "h": 251 }],
			[],
			{}
		],
		[
			1,
			"dan-com",
			2,
			"",
			10,
			[],
			[{ "e": 252 }],
			[{ "v": 252 }],
			[{ "c": 253 }],
			[],
			{}
		],
		[
			1,
			"deepl.com",
			2,
			"",
			22,
			[254],
			[{ "e": 255 }],
			[{ "v": 255 }],
			[{
				"if": { "e": 256 },
				"then": [{ "k": 256 }],
				"else": [{ "h": 255 }]
			}],
			[],
			{}
		],
		[
			1,
			"didomi",
			2,
			"",
			22,
			[257],
			[{ "e": 258 }],
			[{
				"check": "any",
				"v": 259
			}],
			[{
				"if": { "e": 260 },
				"then": [{ "c": 260 }],
				"else": [{ "eval": "EVAL_DIDOMI_OPT_OUT" }]
			}],
			[{ "eval": "EVAL_DIDOMI_TEST" }],
			{}
		],
		[
			1,
			"dmgmedia",
			2,
			"",
			22,
			[261],
			[{ "e": 262 }],
			[{ "v": 262 }],
			[
				{ "c": 263 },
				{ "wv": 264 },
				{
					"all": true,
					"c": 265
				},
				{ "waitForThenClick": ["[data-project=\"mol-fe-cmp\"] [class*=footer]", "xpath///button[contains(., 'Save & Exit')]"] }
			],
			[],
			{}
		],
		[
			1,
			"dmgmedia-us",
			2,
			"",
			22,
			[266],
			[{ "e": 267 }],
			[{ "wv": 267 }],
			[
				{ "c": 268 },
				{ "wv": 269 },
				{ "c": 270 },
				{ "c": 271 }
			],
			[],
			{}
		],
		[
			1,
			"dpgmedia-nl",
			2,
			"",
			22,
			[272],
			[{ "e": 272 }],
			[{ "visible": ["#pg-root-shadow-host", "#pg-modal"] }],
			[{ "waitForThenClick": ["#pg-root-shadow-host", "#pg-configure-btn"] }, { "waitForThenClick": ["#pg-root-shadow-host", "#pg-reject-btn"] }],
			[],
			{}
		],
		[
			1,
			"Drupal",
			2,
			"",
			22,
			[],
			[{ "e": 273 }],
			[{ "v": 273 }],
			[{ "k": 274 }],
			[],
			{}
		],
		[
			1,
			"dunelm.com",
			2,
			"",
			22,
			[275],
			[{ "e": 276 }],
			[{ "v": 276 }],
			[{ "k": 277 }, { "k": 278 }],
			[{ "cc": 279 }, { "cc": 280 }],
			{}
		],
		[
			1,
			"elsevier-pure",
			2,
			"",
			22,
			[281],
			[{ "e": 282 }],
			[{ "v": 282 }],
			[{ "c": 283 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 282
			}],
			{}
		],
		[
			1,
			"Ensighten",
			2,
			"",
			22,
			[284, 285],
			[{ "any": [{ "v": 286 }, { "v": 287 }] }],
			[{ "any": [{ "v": 286 }, { "v": 287 }] }],
			[{ "wait": 500 }, {
				"if": { "v": 286 },
				"then": [{
					"if": {
						"check": "any",
						"v": 288
					},
					"then": [{ "c": 288 }],
					"else": [{
						"if": {
							"check": "any",
							"v": 289
						},
						"then": [
							{ "c": 289 },
							{
								"timeout": 1e3,
								"w": 287
							},
							{
								"all": true,
								"optional": true,
								"k": 290
							},
							{ "c": 291 }
						],
						"else": [{ "c": 292 }]
					}]
				}],
				"else": [{
					"all": true,
					"optional": true,
					"k": 290
				}, { "c": 291 }]
			}],
			[],
			{}
		],
		[
			1,
			"etsy",
			2,
			"",
			22,
			[293, 294],
			[{ "e": 293 }],
			[{ "v": 293 }],
			[
				{ "k": 295 },
				{
					"timeout": 3e3,
					"wv": 296
				},
				{ "wait": 1e3 },
				{ "eval": "EVAL_ETSY_0" },
				{ "eval": "EVAL_ETSY_1" }
			],
			[],
			{}
		],
		[
			1,
			"EU Cookie Law",
			1,
			"",
			22,
			[297],
			[{ "e": 298 }],
			[{ "wait": 500 }, { "v": 298 }],
			[{ "h": 298 }],
			[{
				"negated": true,
				"cc": 299
			}],
			{}
		],
		[
			1,
			"eu-cookie-compliance-banner",
			2,
			"",
			22,
			[],
			[{ "e": 300 }],
			[{ "e": 300 }],
			[{ "k": 301 }],
			[{
				"negated": true,
				"cc": 302
			}],
			{}
		],
		[
			1,
			"EZoic",
			2,
			"",
			22,
			[303],
			[{ "e": 303 }],
			[{ "v": 303 }],
			[
				{ "wait": 500 },
				{ "k": 304 },
				{ "w": 305 },
				{
					"all": true,
					"k": 306
				},
				{ "k": 307 }
			],
			[{ "cc": 308 }],
			{}
		],
		[
			1,
			"fastcmp",
			0,
			"",
			22,
			[309],
			[{ "e": 310 }],
			[{ "v": 310 }],
			[{ "waitForThenClick": ["iframe#fast-cmp-iframe", ".fast-cmp-home-refuse button"] }],
			[{ "cc": 311 }],
			{}
		],
		[
			1,
			"fedex",
			0,
			"",
			22,
			[312],
			[{ "e": 312 }],
			[{ "v": 313 }],
			[{ "c": 314 }],
			[{ "cc": 315 }],
			{}
		],
		[
			1,
			"fides",
			2,
			"",
			22,
			[316],
			[{ "e": 317 }],
			[{ "v": 317 }, { "eval": "EVAL_FIDES_DETECT_POPUP" }],
			[{ "w": 318 }, {
				"if": { "v": 319 },
				"then": [{ "k": 319 }],
				"else": [{ "k": 320 }, { "c": 321 }]
			}],
			[],
			{}
		],
		[
			1,
			"finsweet",
			0,
			"",
			22,
			[322],
			[{ "e": 322 }],
			[{ "v": 323 }],
			[{ "wait": 500 }, {
				"if": { "e": 324 },
				"then": [{ "k": 324 }],
				"else": [{ "h": 322 }]
			}],
			[],
			{}
		],
		[
			1,
			"fullertonhotels.com",
			0,
			"",
			10,
			[],
			[{ "exists": ["#ifrmCookieBanner", "#sp-decline"] }],
			[{ "visible": ["#ifrmCookieBanner", "#sp-decline"] }],
			[{ "wait": 500 }, { "waitForThenClick": ["#ifrmCookieBanner", "#sp-decline"] }],
			[],
			{}
		],
		[
			1,
			"funding-choices",
			2,
			"",
			22,
			[325],
			[{ "e": 326 }],
			[{ "e": 327 }],
			[
				{ "k": 328 },
				{
					"all": true,
					"optional": true,
					"k": 329
				},
				{
					"optional": true,
					"k": 330
				}
			],
			[],
			{}
		],
		[
			1,
			"gallup",
			2,
			"",
			22,
			[331],
			[{ "e": 332 }],
			[{ "v": 332 }],
			[
				{ "c": 332 },
				{ "wait": 500 },
				{ "c": 333 }
			],
			[{ "cc": 334 }],
			{}
		],
		[
			1,
			"gdpr-legal-cookie",
			2,
			"",
			22,
			[335],
			[{ "any": [{ "eval": "EVAL_GDPR_LEGAL_COOKIE_DETECT_CMP" }, { "e": 336 }] }],
			[{
				"check": "any",
				"v": 337
			}],
			[{ "c": 338 }],
			[{ "eval": "EVAL_GDPR_LEGAL_COOKIE_TEST" }],
			{}
		],
		[
			1,
			"google-consent-standalone",
			2,
			"",
			22,
			[],
			[{ "e": 339 }, { "e": 340 }],
			[{ "v": 339 }],
			[{ "c": 341 }],
			[],
			{}
		],
		[
			1,
			"google-cookiebar",
			0,
			"",
			22,
			[342],
			[{ "e": 342 }],
			[{ "v": 342 }],
			[{
				"if": { "e": 343 },
				"then": [{ "k": 343 }],
				"else": [{ "h": 342 }]
			}],
			[],
			{}
		],
		[
			1,
			"google.com",
			2,
			"",
			22,
			[344],
			[{ "e": 344 }, { "e": 345 }],
			[{ "v": 346 }],
			[{ "c": 346 }],
			[{ "cc": 347 }],
			{}
		],
		[
			1,
			"gov.uk",
			2,
			"",
			22,
			[],
			[{ "e": 348 }],
			[{ "e": 349 }],
			[
				{ "wait": 300 },
				{
					"if": { "visible": [".govuk-cookie-banner__message", "xpath///button[contains(., 'Reject')] | //a[contains(., 'Reject')] | //input[contains(@value, 'Reject')] | //button[contains(., 'do not use analytics cookies')]"] },
					"then": [{ "click": [".govuk-cookie-banner__message", "xpath///button[contains(., 'Reject')] | //a[contains(., 'Reject')] | //input[contains(@value, 'Reject')] | //button[contains(., 'do not use analytics cookies')]"] }]
				},
				{ "waitForVisible": [".govuk-cookie-banner__message", "xpath///button[contains(., 'Hide')] | //a[contains(., 'Hide')] | //input[contains(@value, 'Hide')]"] },
				{ "click": [".govuk-cookie-banner__message", "xpath///button[contains(., 'Hide')] | //a[contains(., 'Hide')] | //input[contains(@value, 'Hide')]"] }
			],
			[],
			{}
		],
		[
			1,
			"gravito",
			2,
			"",
			22,
			[350],
			[{ "e": 350 }],
			[{ "v": 350 }],
			[
				{ "c": 351 },
				{ "w": 352 },
				{ "w": 353 },
				{
					"all": true,
					"optional": true,
					"k": 354
				},
				{ "c": 352 }
			],
			[{ "cc": 355 }],
			{}
		],
		[
			1,
			"healthline-media",
			2,
			"",
			22,
			[356],
			[{ "e": 357 }],
			[{ "e": 357 }],
			[{
				"if": { "e": 358 },
				"then": [{ "k": 358 }],
				"else": [{ "wv": 359 }, { "k": 360 }]
			}],
			[],
			{}
		],
		[
			1,
			"hema",
			2,
			"",
			22,
			[100],
			[{ "v": 361 }],
			[{ "v": 361 }],
			[{ "c": 362 }],
			[{ "cc": 363 }],
			{}
		],
		[
			1,
			"hl.co.uk",
			2,
			"",
			22,
			[364, 365],
			[{ "e": 365 }],
			[{ "e": 365 }],
			[
				{ "k": 366 },
				{ "h": 367 },
				{ "w": 368 },
				{
					"optional": true,
					"k": 369
				},
				{ "w": 370 },
				{
					"optional": true,
					"k": 371
				},
				{ "k": 372 }
			],
			[],
			{}
		],
		[
			1,
			"holidaymedia",
			2,
			"",
			22,
			[373],
			[{ "e": 373 }],
			[{ "v": 373 }],
			[{
				"timeout": 2e3,
				"c": 374
			}],
			[],
			{}
		],
		[
			1,
			"hu-manity",
			2,
			"",
			22,
			[375],
			[{ "e": 376 }],
			[{ "v": 376 }],
			[{ "c": 377 }],
			[],
			{}
		],
		[
			1,
			"hubspot",
			2,
			"",
			22,
			[],
			[{ "e": 378 }],
			[{ "v": 378 }],
			[{ "k": 379 }],
			[],
			{}
		],
		[
			1,
			"inmobi",
			2,
			"",
			22,
			[380],
			[{ "e": 381 }],
			[{ "v": 381 }],
			[{ "c": 382 }],
			[{ "cc": 383 }],
			{}
		],
		[
			1,
			"ionos.de",
			2,
			"",
			22,
			[384, 385],
			[{ "e": 385 }],
			[{ "v": 385 }],
			[{ "k": 386 }, { "k": 387 }],
			[],
			{}
		],
		[
			1,
			"iubenda",
			2,
			"",
			22,
			[388],
			[{ "e": 388 }],
			[{ "v": 389 }],
			[{
				"if": { "e": 390 },
				"then": [{ "k": 390 }],
				"else": [
					{ "c": 391 },
					{ "c": 392 },
					{ "c": 393 }
				]
			}],
			[{ "eval": "EVAL_IUBENDA_1" }],
			{}
		],
		[
			1,
			"iWink",
			2,
			"",
			22,
			[394],
			[{ "e": 394 }],
			[{ "v": 394 }],
			[{ "c": 395 }],
			[{ "cc": 396 }],
			{}
		],
		[
			1,
			"jetpack-eu-cookie-law",
			1,
			"",
			22,
			[397],
			[{ "e": 397 }],
			[{ "v": 397 }],
			[{ "h": 397 }],
			[],
			{}
		],
		[
			1,
			"johnlewis.com",
			2,
			"",
			22,
			[398],
			[{ "e": 398 }],
			[{ "e": 398 }],
			[
				{ "k": 399 },
				{ "wait": 500 },
				{
					"all": true,
					"optional": true,
					"k": 400
				},
				{ "k": 401 }
			],
			[],
			{}
		],
		[
			1,
			"jquery.cookieBar",
			1,
			"",
			22,
			[402],
			[{ "e": 403 }],
			[{
				"check": "any",
				"v": 403
			}],
			[{ "h": 402 }],
			[{
				"check": "none",
				"v": 403
			}, {
				"negated": true,
				"cc": 404
			}],
			{}
		],
		[
			1,
			"justwatch.com",
			2,
			"",
			22,
			[405],
			[{ "e": 406 }],
			[{ "v": 406 }],
			[
				{ "k": 407 },
				{ "c": 408 },
				{ "c": 409 },
				{
					"all": true,
					"optional": true,
					"k": 410
				},
				{ "wv": 411 },
				{ "k": 411 }
			],
			[],
			{}
		],
		[
			1,
			"kconsent",
			0,
			"",
			10,
			[412],
			[{ "e": 413 }],
			[{ "v": 414 }],
			[{ "c": 415 }],
			[],
			{}
		],
		[
			1,
			"ketch",
			2,
			"",
			10,
			[
				416,
				417,
				418
			],
			[{ "any": [
				{ "e": 416 },
				{ "e": 417 },
				{ "e": 418 },
				{ "e": 419 }
			] }],
			[{ "any": [
				{ "v": 416 },
				{ "v": 417 },
				{ "v": 418 }
			] }],
			[
				{
					"if": { "e": 420 },
					"then": [{ "c": 420 }]
				},
				{
					"if": { "e": 421 },
					"then": [{
						"if": { "e": 422 },
						"then": [{ "c": 422 }],
						"else": [{
							"if": { "e": 423 },
							"then": [{ "k": 423 }]
						}]
					}]
				},
				{
					"timeout": 1e3,
					"optional": true,
					"w": 424
				},
				{
					"if": { "e": 424 },
					"then": [{ "c": 425 }, { "k": 426 }]
				}
			],
			[{ "cc": 427 }],
			{ "intermediate": false }
		],
		[
			1,
			"lia",
			2,
			"",
			22,
			[428],
			[{ "e": 428 }],
			[{ "e": 429 }, { "v": 428 }],
			[{ "c": 430 }],
			[],
			{}
		],
		[
			1,
			"lightbox",
			2,
			"",
			22,
			[431],
			[{ "e": 432 }],
			[{ "v": 432 }],
			[{ "k": 433 }],
			[],
			{}
		],
		[
			1,
			"lineagrafica",
			1,
			"",
			22,
			[434],
			[{ "e": 434 }],
			[{ "e": 434 }],
			[{ "h": 434 }],
			[],
			{}
		],
		[
			1,
			"linkedin.com",
			2,
			"",
			22,
			[435],
			[{ "e": 435 }],
			[{ "v": 435 }],
			[
				{ "wv": 436 },
				{ "wait": 500 },
				{ "c": 436 }
			],
			[{
				"check": "none",
				"wv": 435
			}],
			{}
		],
		[
			1,
			"macaron",
			2,
			"",
			22,
			[437],
			[{ "e": 437 }],
			[{ "v": 438 }],
			[{
				"if": { "e": 439 },
				"then": [{ "k": 439 }],
				"else": [
					{ "c": 440 },
					{ "w": 441 },
					{ "wv": 442 },
					{
						"all": true,
						"optional": true,
						"c": 443
					},
					{ "c": 444 }
				]
			}],
			[{ "cc": 445 }],
			{}
		],
		[
			1,
			"mediamarkt.de",
			2,
			"",
			22,
			[446, 447],
			[{ "e": 448 }],
			[{ "e": 448 }],
			[{ "k": 449 }],
			[],
			{}
		],
		[
			1,
			"microsoft.com",
			2,
			"",
			22,
			[450],
			[{ "e": 450 }],
			[{ "e": 450 }],
			[{ "eval": "EVAL_MICROSOFT_0" }],
			[{ "eval": "EVAL_MICROSOFT_2" }],
			{}
		],
		[
			1,
			"moneysavingexpert.com",
			2,
			"",
			22,
			[],
			[{ "e": 451 }],
			[{ "v": 451 }],
			[{ "k": 452 }, { "k": 453 }],
			[],
			{}
		],
		[
			1,
			"Moove",
			2,
			"",
			22,
			[454],
			[{ "e": 454 }],
			[{ "v": 455 }],
			[{
				"if": { "e": 456 },
				"then": [{ "k": 456 }],
				"else": [{
					"if": { "e": 457 },
					"then": [
						{ "k": 457 },
						{ "wv": 458 },
						{ "eval": "EVAL_MOOVE_0" },
						{ "k": 459 }
					],
					"else": [{ "h": 454 }]
				}]
			}],
			[{
				"check": "none",
				"v": 454
			}],
			{}
		],
		[
			1,
			"nhs.uk",
			2,
			"",
			22,
			[460],
			[{ "e": 460 }],
			[{ "e": 460 }],
			[{ "k": 461 }],
			[],
			{}
		],
		[
			1,
			"obi.de",
			2,
			"",
			22,
			[462],
			[{ "e": 463 }],
			[{ "v": 463 }],
			[{ "k": 464 }],
			[],
			{}
		],
		[
			1,
			"om",
			2,
			"",
			22,
			[465],
			[{ "e": 466 }],
			[{ "e": 466 }],
			[{
				"if": { "e": 467 },
				"then": [{ "c": 467 }],
				"else": [{
					"all": true,
					"optional": true,
					"k": 468
				}, { "c": 469 }]
			}],
			[],
			{}
		],
		[
			1,
			"openli",
			2,
			"",
			22,
			[470],
			[{ "e": 470 }],
			[{
				"check": "any",
				"v": 471
			}],
			[{ "c": 472 }],
			[],
			{}
		],
		[
			1,
			"osano",
			2,
			"",
			22,
			[473],
			[{ "e": 474 }],
			[{ "eval": "EVAL_OSANO_DETECT" }, { "v": 475 }],
			[{
				"if": { "e": 476 },
				"then": [{ "k": 476 }],
				"else": [{ "h": 473 }]
			}],
			[],
			{}
		],
		[
			1,
			"otto.de",
			2,
			"",
			22,
			[477],
			[{ "e": 477 }],
			[{ "v": 478 }],
			[{ "k": 479 }],
			[],
			{}
		],
		[
			1,
			"overleaf",
			2,
			"",
			22,
			[480],
			[{ "e": 481 }],
			[{ "v": 481 }],
			[{ "c": 482 }],
			[{ "cc": 483 }],
			{}
		],
		[
			1,
			"pabcogypsum",
			2,
			"",
			22,
			[484],
			[{ "e": 485 }],
			[{ "v": 485 }],
			[{ "c": 486 }],
			[],
			{}
		],
		[
			1,
			"pandectes",
			2,
			"",
			22,
			[487],
			[{ "e": 487 }],
			[{ "v": 487 }],
			[{
				"if": { "v": 488 },
				"then": [{ "k": 488 }],
				"else": [
					{ "k": 489 },
					{ "c": 490 },
					{ "k": 491 }
				]
			}],
			[{ "wait": 500 }, { "eval": "EVAL_PANDECTES_TEST" }],
			{}
		],
		[
			1,
			"paypal-us",
			2,
			"",
			22,
			[492],
			[{ "e": 493 }],
			[{ "v": 493 }],
			[{
				"if": { "e": 494 },
				"then": [{ "k": 494 }],
				"else": [{
					"if": { "e": 495 },
					"then": [{ "k": 495 }],
					"else": [
						{ "wv": 496 },
						{
							"all": true,
							"optional": true,
							"k": 497
						},
						{ "k": 498 }
					]
				}]
			}],
			[],
			{}
		],
		[
			1,
			"paypal.com",
			2,
			"",
			22,
			[499],
			[{ "e": 499 }],
			[{ "v": 500 }],
			[{ "c": 494 }],
			[{ "wait": 500 }, { "cc": 501 }],
			{}
		],
		[
			1,
			"pmc",
			1,
			"",
			22,
			[502],
			[{ "e": 502 }],
			[{ "v": 502 }],
			[{ "h": 502 }],
			[],
			{}
		],
		[
			1,
			"pornhat",
			1,
			"",
			22,
			[503],
			[{ "v": 504 }],
			[{ "v": 504 }],
			[{ "h": 503 }],
			[],
			{}
		],
		[
			1,
			"pride.com",
			1,
			"",
			22,
			[505],
			[{ "e": 506 }],
			[{ "v": 506 }],
			[{ "h": 505 }],
			[],
			{}
		],
		[
			1,
			"PrimeBox CookieBar",
			2,
			"",
			22,
			[507],
			[{ "e": 508 }],
			[{
				"check": "any",
				"v": 508
			}],
			[{
				"optional": true,
				"k": 509
			}, { "h": 507 }],
			[{
				"negated": true,
				"cc": 510
			}],
			{}
		],
		[
			1,
			"privado",
			0,
			"",
			10,
			[511],
			[{ "e": 512 }],
			[{ "v": 512 }],
			[{
				"if": { "e": 513 },
				"then": [{ "c": 513 }],
				"else": [
					{ "c": 514 },
					{ "wv": 515 },
					{ "c": 515 }
				]
			}],
			[],
			{}
		],
		[
			1,
			"pubtech",
			2,
			"",
			22,
			[516],
			[{ "e": 516 }],
			[{ "v": 517 }],
			[{ "k": 518 }],
			[{ "eval": "EVAL_PUBTECH_0" }],
			{}
		],
		[
			1,
			"quantcast",
			2,
			"",
			22,
			[519],
			[{ "e": 520 }],
			[{ "v": 521 }],
			[{
				"if": { "e": 522 },
				"then": [{ "k": 522 }],
				"else": [{
					"timeout": 2e3,
					"w": 523
				}, {
					"if": { "e": 524 },
					"then": [{ "k": 524 }],
					"else": [
						{ "k": 525 },
						{ "wv": 526 },
						{
							"all": true,
							"optional": true,
							"k": 527
						},
						{
							"click": [".qc-cmp2-main", "xpath///button[contains(., 'REJECT ALL') or contains(., 'ALLE VERWERPEN') or contains(., 'ΑΠΟΡΡΙΠΤΩ ΤΑ ΠΑΝΤΑ') or contains(., 'RESPINGERE TOTALĂ') or contains(., 'ALLE ABLEHNEN') or contains(., 'ODRZUCENIE') or contains(., 'BLOQUEAR TODO') or contains(., 'REJEITAR TODOS') or contains(., 'RIFIUTA TUTTO') or contains(., 'TOUT REFUSER') or contains(., 'ОТКЛОНИТЬ ВСЕХ')]"],
							"optional": true
						},
						{ "wait": 500 },
						{
							"if": { "e": 528 },
							"then": [{ "k": 528 }],
							"else": [{
								"waitForThenClick": [".qc-cmp2-main", "xpath///button[contains(.,'SAVE & EXIT') or contains(.,'SALVA ED ESCI') or contains(.,'GUARDAR Y SALIR') or contains(.,'SPEICHERN & VERLASSEN')"],
								"timeout": 5e3
							}]
						}
					]
				}]
			}],
			[],
			{}
		],
		[
			1,
			"real-cookie-banner",
			0,
			"",
			10,
			[529],
			[{ "e": 530 }],
			[{ "v": 530 }],
			[{ "waitForThenClick": ["div[consent-skip-blocker=\"1\"][id][data-bg] > dialog > div > div > div > div > div > a[role=button]:not([id])", "xpath///span[contains(., ' ohne ') or contains(., 'without') or contains(., 'Ablehnen')]"] }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 530
			}],
			{}
		],
		[
			1,
			"ring",
			2,
			"",
			22,
			[],
			[{ "e": 531 }],
			[{ "v": 531 }],
			[
				{ "c": 532 },
				{ "wv": 533 },
				{
					"all": true,
					"optional": true,
					"k": 534
				},
				{ "c": 535 }
			],
			[],
			{}
		],
		[
			1,
			"sandhills",
			1,
			"",
			22,
			[536],
			[{ "e": 537 }],
			[{ "v": 537 }],
			[{ "h": 536 }],
			[],
			{}
		],
		[
			1,
			"sas",
			2,
			"",
			22,
			[538],
			[{ "e": 538 }],
			[{ "v": 538 }],
			[{ "c": 539 }],
			[{ "cc": 540 }],
			{}
		],
		[
			1,
			"shopify",
			0,
			"",
			22,
			[541],
			[{ "e": 541 }],
			[{ "v": 541 }],
			[{ "c": 542 }],
			[{ "eval": "EVAL_SHOPIFY_TEST" }],
			{}
		],
		[
			1,
			"sibbo",
			2,
			"",
			22,
			[543],
			[{ "e": 543 }],
			[{ "v": 544 }],
			[{ "k": 544 }],
			[],
			{}
		],
		[
			1,
			"Sirdata",
			0,
			"",
			22,
			[545],
			[{ "e": 545 }],
			[{ "v": 545 }],
			[{
				"if": { "exists": ["#sd-cmp", "xpath///button[contains(., 'Do not accept') or contains(., 'Acceptera inte') or contains(., 'No aceptar') or contains(., 'Ikke acceptere') or contains(., 'Nicht akzeptieren') or contains(., 'Не приемам') or contains(., 'Να μην γίνει αποδοχή') or contains(., 'Niet accepteren') or contains(., 'Nepřijímat') or contains(., 'Nie akceptuj') or contains(., 'Nu acceptați') or contains(., 'Não aceitar') or contains(., 'Continuer sans accepter') or contains(., 'Non accettare') or contains(., 'Nem fogad el')]"] },
				"then": [{ "waitForThenClick": ["#sd-cmp", "xpath///button[contains(., 'Do not accept') or contains(., 'Acceptera inte') or contains(., 'No aceptar') or contains(., 'Ikke acceptere') or contains(., 'Nicht akzeptieren') or contains(., 'Не приемам') or contains(., 'Να μην γίνει αποδοχή') or contains(., 'Niet accepteren') or contains(., 'Nepřijímat') or contains(., 'Nie akceptuj') or contains(., 'Nu acceptați') or contains(., 'Não aceitar') or contains(., 'Continuer sans accepter') or contains(., 'Non accettare') or contains(., 'Nem fogad el')]"] }],
				"else": [{
					"if": { "exists": ["#sd-cmp", "xpath///span[contains(., 'Задайте вашите избори') or contains(., 'Nastavit vaše volby') or contains(., 'Angiv dine valg') or contains(., 'Ihre Auswahl treffen') or contains(., 'Ορίστε τις επιλογές σας') or contains(., 'Set your choices') or contains(., 'Establecer preferencias') or contains(., 'Paramétrer vos choix') or contains(., 'Válassza ki a beállításokat') or contains(., 'Imposta le tue scelte') or contains(., 'Stel uw keuzes in') or contains(., 'Ustaw swoje wybory') or contains(., 'Definir suas escolhas') or contains(., 'Setați-vă opțiunile') or contains(., 'Ställ in dina val')]"] },
					"then": [{ "waitForThenClick": ["#sd-cmp", "xpath///span[contains(., 'Задайте вашите избори') or contains(., 'Nastavit vaše volby') or contains(., 'Angiv dine valg') or contains(., 'Ihre Auswahl treffen') or contains(., 'Ορίστε τις επιλογές σας') or contains(., 'Set your choices') or contains(., 'Establecer preferencias') or contains(., 'Paramétrer vos choix') or contains(., 'Válassza ki a beállításokat') or contains(., 'Imposta le tue scelte') or contains(., 'Stel uw keuzes in') or contains(., 'Ustaw swoje wybory') or contains(., 'Definir suas escolhas') or contains(., 'Setați-vă opțiunile') or contains(., 'Ställ in dina val')]"] }, { "waitForThenClick": ["#sd-cmp", "xpath///span[contains(., 'Отхвърлям всичко') or contains(., 'Odmítnout vše') or contains(., 'Afvis alt') or contains(., 'Alle ablehnen') or contains(., 'Απόρριψη όλων') or contains(., 'Reject all') or contains(., 'Rechazar todo') or contains(., 'Tout refuser') or contains(., 'Mind elutasítása') or contains(., 'Rifiuta tutto') or contains(., 'Alles weigeren') or contains(., 'Odrzuć wszystkie') or contains(., 'Rejeitar todos') or contains(., 'Respingeți toate') or contains(., 'Avvisa allt')]"] }],
					"else": [{ "c": 546 }]
				}]
			}],
			[],
			{}
		],
		[
			1,
			"snigel",
			2,
			"",
			22,
			[],
			[{ "e": 547 }],
			[{ "v": 547 }],
			[{ "k": 548 }, { "k": 549 }],
			[{ "cc": 550 }],
			{}
		],
		[
			1,
			"squarespace-cookie-banner",
			2,
			"",
			22,
			[551],
			[{ "e": 552 }],
			[{ "v": 552 }],
			[{ "c": 553 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 552
			}],
			{}
		],
		[
			1,
			"squiz",
			0,
			"",
			10,
			[511],
			[{ "e": 554 }],
			[{ "v": 554 }],
			[{ "c": 555 }],
			[{ "wait": 500 }, { "cc": 556 }],
			{}
		],
		[
			1,
			"steampowered.com",
			2,
			"",
			22,
			[],
			[{ "e": 557 }, { "v": 557 }],
			[{ "v": 557 }],
			[{ "k": 558 }],
			[{ "wait": 1e3 }, { "eval": "EVAL_STEAMPOWERED_0" }],
			{}
		],
		[
			1,
			"stripchat.com",
			0,
			"",
			22,
			[559],
			[{ "e": 560 }],
			[{ "v": 560 }],
			[{ "c": 561 }, { "c": 562 }],
			[{ "wait": 500 }, { "cc": 563 }],
			{}
		],
		[
			1,
			"substack",
			0,
			"",
			12,
			[],
			[{ "e": 564 }],
			[{ "v": 564 }],
			[{ "waitForThenClick": [".pencraft", "xpath///button[contains(., 'Only Necessary') or contains(., 'Reject')]"] }],
			[{ "cc": 565 }],
			{}
		],
		[
			1,
			"summitracing",
			1,
			"",
			10,
			[566],
			[{ "e": 567 }],
			[{ "v": 567 }],
			[{ "h": 566 }],
			[],
			{}
		],
		[
			1,
			"synology",
			0,
			"",
			22,
			[568],
			[{ "e": 568 }],
			[{ "v": 568 }],
			[
				{ "c": 569 },
				{ "wv": 570 },
				{
					"all": true,
					"optional": true,
					"k": 571
				},
				{ "c": 572 }
			],
			[{ "cc": 573 }, { "cc": 574 }],
			{}
		],
		[
			1,
			"takealot.com",
			1,
			"",
			22,
			[575],
			[{ "e": 576 }],
			[{ "e": 576 }],
			[{ "h": 575 }, {
				"if": { "e": 577 },
				"then": [{ "eval": "EVAL_TAKEALOT_0" }],
				"else": []
			}],
			[],
			{}
		],
		[
			1,
			"tarteaucitron deny",
			2,
			"",
			22,
			[578],
			[{ "e": 578 }],
			[{ "v": 579 }],
			[{ "wait": 500 }, { "k": 579 }],
			[{ "eval": "EVAL_TARTEAUCITRON_2" }],
			{}
		],
		[
			1,
			"tarteaucitron.js",
			2,
			"",
			22,
			[578],
			[{ "e": 578 }],
			[{ "v": 580 }, {
				"negated": true,
				"e": 579
			}],
			[{
				"if": { "e": 581 },
				"then": [{
					"all": true,
					"optional": true,
					"k": 582
				}, { "k": 580 }],
				"else": [
					{ "k": 583 },
					{ "c": 584 },
					{ "k": 585 }
				]
			}],
			[{ "eval": "EVAL_TARTEAUCITRON_2" }],
			{}
		],
		[
			1,
			"taunton",
			2,
			"",
			22,
			[586],
			[{ "e": 586 }],
			[{ "e": 587 }],
			[{
				"optional": true,
				"all": true,
				"k": 588
			}, { "k": 589 }],
			[{ "cc": 590 }],
			{}
		],
		[
			1,
			"tccCmpAlert",
			2,
			"",
			22,
			[591],
			[{ "e": 591 }],
			[{ "v": 591 }],
			[{ "c": 592 }],
			[],
			{}
		],
		[
			1,
			"Tealium",
			2,
			"",
			22,
			[593],
			[{ "e": 594 }, { "eval": "EVAL_TEALIUM_0" }],
			[{
				"check": "any",
				"v": 594
			}],
			[
				{ "eval": "EVAL_TEALIUM_1" },
				{ "eval": "EVAL_TEALIUM_DONOTSELL" },
				{ "h": 595 },
				{
					"timeout": 1e3,
					"optional": true,
					"c": 596
				}
			],
			[
				{ "eval": "EVAL_TEALIUM_3" },
				{ "eval": "EVAL_TEALIUM_DONOTSELL_CHECK" },
				{
					"check": "none",
					"v": 597
				}
			],
			{}
		],
		[
			1,
			"Termly",
			2,
			"",
			22,
			[598],
			[{ "e": 598 }],
			[{ "v": 599 }],
			[{
				"if": { "e": 600 },
				"then": [{ "k": 600 }],
				"else": [
					{ "c": 601 },
					{
						"timeout": 700,
						"w": 602
					},
					{
						"if": { "e": 602 },
						"then": [{ "k": 602 }],
						"else": [{
							"all": true,
							"c": 603
						}, { "c": 604 }]
					}
				]
			}],
			[],
			{}
		],
		[
			1,
			"termsfeed",
			2,
			"",
			22,
			[605],
			[{ "e": 605 }],
			[{ "v": 605 }],
			[{
				"if": { "e": 606 },
				"then": [{ "c": 606 }],
				"else": [
					{ "c": 607 },
					{ "w": 608 },
					{
						"all": true,
						"optional": true,
						"k": 609
					},
					{ "k": 610 }
				]
			}],
			[],
			{}
		],
		[
			1,
			"termsfeed3",
			2,
			"",
			22,
			[611],
			[{ "e": 612 }],
			[{ "v": 612 }],
			[{
				"if": { "e": 613 },
				"then": [
					{ "k": 613 },
					{ "wv": 614 },
					{ "c": 614 }
				],
				"else": [{ "h": 611 }]
			}],
			[],
			{}
		],
		[
			1,
			"Test page CMP",
			2,
			"",
			22,
			[615],
			[{ "e": 616 }],
			[{ "v": 616 }],
			[
				{ "w": 615 },
				{ "eval": "EVAL_TESTCMP_STEP" },
				{ "k": 615 }
			],
			[{ "eval": "EVAL_TESTCMP_0" }],
			{}
		],
		[
			1,
			"Test page cosmetic CMP",
			1,
			"",
			22,
			[617],
			[{ "e": 618 }],
			[{ "v": 618 }],
			[{ "h": 618 }],
			[{ "wait": 500 }, { "eval": "EVAL_TESTCMP_COSMETIC_0" }],
			{}
		],
		[
			1,
			"thalia.de",
			2,
			"",
			22,
			[619],
			[{ "e": 620 }],
			[{ "v": 619 }],
			[{ "k": 621 }],
			[],
			{}
		],
		[
			1,
			"thefreedictionary.com",
			2,
			"",
			22,
			[622],
			[{ "e": 622 }],
			[{ "v": 622 }],
			[{ "eval": "EVAL_THEFREEDICTIONARY_0" }],
			[],
			{}
		],
		[
			1,
			"toyota",
			0,
			"",
			10,
			[623],
			[{ "e": 624 }],
			[{ "v": 624 }],
			[{ "c": 625 }],
			[{ "cc": 626 }],
			{}
		],
		[
			1,
			"tplink",
			0,
			"",
			22,
			[627],
			[{ "e": 627 }],
			[{ "v": 628 }],
			[{
				"all": true,
				"optional": true,
				"k": 629
			}, { "c": 630 }],
			[{ "cc": 631 }],
			{}
		],
		[
			1,
			"trader-joes-com",
			1,
			"",
			22,
			[632],
			[{ "e": 632 }],
			[{ "v": 632 }],
			[{ "h": 632 }],
			[],
			{}
		],
		[
			2,
			"transcend",
			1,
			"",
			22,
			[633],
			[{ "e": 633 }],
			[{ "v": 633 }],
			[{
				"setStyle": "display: none !important; z-index: -1 !important; pointer-events: none !important;",
				"selector": "#transcend-consent-manager"
			}],
			[],
			{}
		],
		[
			1,
			"tropicfeel-com",
			2,
			"",
			22,
			[634],
			[{ "e": 634 }],
			[{
				"check": "any",
				"v": 635
			}],
			[
				{ "k": 636 },
				{ "w": 637 },
				{
					"all": true,
					"k": 638
				},
				{ "k": 639 }
			],
			[],
			{}
		],
		[
			1,
			"truyo",
			2,
			"",
			22,
			[640],
			[{ "e": 641 }],
			[{ "v": 640 }],
			[{ "k": 642 }],
			[],
			{}
		],
		[
			1,
			"twcc",
			0,
			"",
			10,
			[643],
			[{ "e": 644 }],
			[{ "v": 644 }],
			[{ "c": 645 }],
			[{ "cc": 646 }],
			{}
		],
		[
			1,
			"u12-data-protection-notice",
			2,
			"",
			22,
			[647],
			[{ "e": 647 }],
			[{ "v": 648 }],
			[{ "c": 649 }],
			[],
			{}
		],
		[
			1,
			"ubuntu.com",
			2,
			"",
			22,
			[650],
			[{ "any": [{ "e": 651 }, { "e": 652 }] }],
			[{ "any": [{ "v": 653 }, { "v": 652 }] }],
			[
				{ "any": [{ "c": 654 }, { "c": 655 }] },
				{
					"optional": true,
					"all": true,
					"timeout": 500,
					"c": 656
				},
				{ "any": [{ "c": 657 }, { "c": 658 }] }
			],
			[{ "cc": 659 }],
			{}
		],
		[
			1,
			"UK Cookie Consent",
			1,
			"",
			22,
			[660],
			[{ "e": 660 }],
			[{ "e": 661 }],
			[{ "h": 660 }],
			[{
				"negated": true,
				"cc": 662
			}],
			{}
		],
		[
			1,
			"usercentrics-api",
			2,
			"",
			22,
			[],
			[{ "e": 663 }],
			[{ "eval": "EVAL_USERCENTRICS_API_0" }, {
				"if": { "e": 664 },
				"then": [{
					"timeout": 2e3,
					"wv": 664
				}],
				"else": [{ "exists": ["#usercentrics-root", "[data-testid=uc-container]"] }, {
					"timeout": 2e3,
					"wv": 665
				}]
			}],
			[{
				"if": { "exists": ["#usercentrics-root", "[data-testid=uc-deny-all-button]"] },
				"then": [{ "click": ["#usercentrics-root", "[data-testid=uc-deny-all-button]"] }],
				"else": [{
					"if": { "exists": ["#usercentrics-cmp-ui", "[data-action-type=deny]"] },
					"then": [{ "click": ["#usercentrics-cmp-ui", "[data-action-type=deny]"] }],
					"else": [{ "eval": "EVAL_USERCENTRICS_API_1" }, { "eval": "EVAL_USERCENTRICS_API_2" }]
				}]
			}, {
				"removeClass": "overflowHidden",
				"selector": "body",
				"optional": true
			}],
			[{ "eval": "EVAL_USERCENTRICS_API_6" }],
			{}
		],
		[
			1,
			"usercentrics-button",
			2,
			"",
			22,
			[],
			[{ "e": 666 }],
			[{ "v": 667 }],
			[{ "k": 668 }],
			[{ "eval": "EVAL_USERCENTRICS_BUTTON_0" }],
			{}
		],
		[
			1,
			"waitrose.com",
			2,
			"",
			22,
			[
				669,
				670,
				671
			],
			[{ "e": 670 }],
			[{ "v": 670 }],
			[
				{ "k": 672 },
				{ "wait": 200 },
				{ "eval": "EVAL_WAITROSE_0" },
				{ "k": 673 }
			],
			[{ "cc": 674 }, { "cc": 675 }],
			{}
		],
		[
			1,
			"webflow",
			2,
			"",
			22,
			[676],
			[{ "v": 676 }],
			[{ "v": 676 }, { "v": 677 }],
			[{
				"if": { "e": 678 },
				"then": [{ "wait": 500 }, { "c": 678 }],
				"else": [{ "h": 679 }]
			}],
			[{ "cc": 680 }],
			{}
		],
		[
			1,
			"wiki.gg",
			1,
			"",
			22,
			[681],
			[{ "e": 681 }],
			[{ "v": 681 }],
			[{ "h": 681 }],
			[],
			{}
		],
		[
			1,
			"wix",
			0,
			"",
			22,
			[],
			[{ "e": 682 }],
			[{ "v": 682 }],
			[{
				"if": { "e": 683 },
				"then": [{ "k": 683 }],
				"else": [{ "h": 684 }]
			}],
			[],
			{}
		],
		[
			1,
			"woo-commerce-com",
			2,
			"",
			22,
			[685],
			[{ "e": 685 }],
			[{ "e": 685 }],
			[
				{ "k": 686 },
				{
					"all": true,
					"c": 41
				},
				{ "k": 687 }
			],
			[],
			{}
		],
		[
			1,
			"WP Cookie Notice for GDPR",
			2,
			"",
			22,
			[688],
			[{ "e": 688 }],
			[{ "v": 688 }],
			[{ "c": 689 }],
			[{ "cc": 690 }],
			{}
		],
		[
			1,
			"WP DSGVO Tools",
			2,
			"",
			22,
			[691],
			[{ "e": 692 }],
			[{
				"check": "any",
				"v": 692
			}],
			[{ "c": 693 }],
			[{
				"negated": true,
				"cc": 694
			}],
			{}
		],
		[
			1,
			"wpcc",
			1,
			"",
			22,
			[695],
			[{ "e": 695 }],
			[{ "e": 696 }],
			[{ "h": 695 }],
			[],
			{}
		],
		[
			1,
			"xgroovy",
			1,
			"",
			22,
			[697],
			[{ "e": 698 }],
			[{ "v": 698 }],
			[{ "h": 697 }],
			[],
			{}
		],
		[
			1,
			"xing.com",
			2,
			"",
			22,
			[],
			[{ "e": 699 }],
			[{ "e": 699 }],
			[{ "k": 700 }, { "k": 701 }],
			[{ "cc": 702 }],
			{}
		],
		[
			1,
			"xnxx-com",
			1,
			"",
			22,
			[703],
			[{ "e": 703 }],
			[{ "v": 703 }],
			[{ "h": 703 }],
			[],
			{}
		],
		[
			1,
			"youporn.com",
			1,
			"",
			22,
			[704],
			[{ "e": 705 }],
			[{ "e": 704 }],
			[{ "h": 705 }],
			[],
			{}
		],
		[
			1,
			"youtube-desktop",
			2,
			"",
			22,
			[706, 707],
			[{ "e": 708 }, { "e": 709 }],
			[{ "v": 708 }],
			[{ "c": 710 }, { "wait": 500 }],
			[{ "wait": 500 }, { "cc": 347 }],
			{}
		],
		[
			1,
			"youtube-mobile",
			2,
			"",
			22,
			[711],
			[{ "e": 712 }],
			[{ "v": 712 }],
			[{ "c": 713 }, { "wait": 500 }],
			[{ "wait": 500 }, { "cc": 347 }],
			{}
		],
		[
			1,
			"zdf",
			2,
			"",
			22,
			[714],
			[{ "e": 714 }],
			[{ "v": 715 }],
			[{ "c": 716 }],
			[],
			{}
		],
		[
			1,
			"cookiealert",
			2,
			"",
			11,
			[],
			[{ "e": 717 }],
			[{ "v": 718 }],
			[
				{ "k": 719 },
				{
					"all": true,
					"optional": true,
					"k": 720
				},
				{ "k": 721 },
				{ "eval": "EVAL_COOKIEALERT_0" }
			],
			[{ "eval": "EVAL_COOKIEALERT_2" }],
			{ "intermediate": false }
		],
		[
			1,
			"auto_AU_help.dropbox.com_4ad_+1",
			0,
			"^https?://(www\\.)?dropbox\\.com/|^https?://(www\\.)?dropbox\\.com/",
			1,
			[],
			[{ "e": 722 }],
			[{ "v": 722 }],
			[{ "wait": 500 }, { "c": 722 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 722
			}],
			{}
		],
		[
			1,
			"auto_CA_coasthotels.com_f8m",
			0,
			"^https?://(www\\.)?coasthotels\\.com/",
			1,
			[],
			[{ "e": 723 }],
			[{ "v": 723 }],
			[{ "c": 723 }],
			[],
			{}
		],
		[
			1,
			"auto_CA_epihunter.eu_hd4",
			0,
			"^https?://(www\\.)?combell\\.com/",
			1,
			[],
			[{ "e": 724 }],
			[{ "v": 724 }],
			[{ "wait": 500 }, { "c": 724 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 724
			}],
			{}
		],
		[
			1,
			"auto_CA_nationalarchives.gov.uk_rx0",
			0,
			"^https?://(www\\.)?webarchive\\.nationalarchives\\.gov\\.uk/",
			1,
			[],
			[{ "e": 725 }],
			[{ "v": 725 }],
			[{ "wait": 500 }, { "c": 725 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 725
			}],
			{}
		],
		[
			1,
			"auto_CA_natureconservancy.ca_3pp",
			0,
			"^https?://(www\\.)?natureconservancy\\.ca/",
			1,
			[],
			[{ "e": 726 }],
			[{ "v": 726 }],
			[{ "wait": 500 }, { "c": 726 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 726
			}],
			{}
		],
		[
			1,
			"auto_CA_ontariospca.ca_k32",
			0,
			"^https?://(www\\.)?widget-next\\.clym-sdk\\.net/",
			1,
			[],
			[{ "e": 727 }],
			[{ "v": 727 }],
			[{ "wait": 500 }, { "c": 727 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 727
			}],
			{}
		],
		[
			1,
			"auto_CA_phoenixnap.com_hrb",
			0,
			"^https?://(www\\.)?widget-next\\.clym-sdk\\.net/",
			1,
			[],
			[{ "e": 728 }],
			[{ "v": 728 }],
			[{ "wait": 500 }, { "c": 728 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 728
			}],
			{}
		],
		[
			1,
			"auto_CH_phoenixnap.com_lx5",
			0,
			"^https?://(www\\.)?widget-next\\.clym-sdk\\.net/",
			1,
			[],
			[{ "e": 729 }],
			[{ "v": 729 }],
			[{ "wait": 500 }, { "c": 729 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 729
			}],
			{}
		],
		[
			1,
			"auto_CH_swisscare.com_arx",
			0,
			"^https?://(www\\.)?swisscare\\.com/",
			1,
			[],
			[{ "e": 730 }],
			[{ "v": 730 }],
			[{ "wait": 500 }, { "c": 730 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 730
			}],
			{}
		],
		[
			1,
			"auto_DE_alfaromeo.de_gvg_+2",
			0,
			"^https?://(www\\.)?cookielaw\\.emea\\.fcagroup\\.com/|^https?://(www\\.)?cookielaw\\.emea\\.fcagroup\\.com/|^https?://(www\\.)?cookielaw\\.emea\\.fcagroup\\.com/",
			1,
			[],
			[{ "e": 731 }],
			[{ "v": 731 }],
			[{ "wait": 500 }, { "c": 731 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 731
			}],
			{}
		],
		[
			1,
			"auto_DE_huss-licht-ton.de_jj0",
			0,
			"^https?://(www\\.)?huss-licht-ton\\.de/",
			1,
			[],
			[{ "e": 732 }],
			[{ "v": 732 }],
			[{ "wait": 500 }, { "c": 732 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 732
			}],
			{}
		],
		[
			1,
			"auto_DE_modellbau-berlinski.de_8vm",
			0,
			"^https?://(www\\.)?modellbau-berlinski\\.de/",
			1,
			[],
			[{ "e": 733 }],
			[{ "v": 733 }],
			[{ "wait": 500 }, { "c": 733 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 733
			}],
			{}
		],
		[
			1,
			"auto_DE_phoenixnap.com_xq7",
			0,
			"^https?://(www\\.)?widget-next\\.clym-sdk\\.net/",
			1,
			[],
			[{ "e": 734 }],
			[{ "v": 734 }],
			[{ "wait": 500 }, { "c": 734 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 734
			}],
			{}
		],
		[
			1,
			"auto_FR_fiat.fr_3fh",
			0,
			"^https?://(www\\.)?cookielaw\\.emea\\.fcagroup\\.com/",
			1,
			[],
			[{ "e": 735 }],
			[{ "v": 735 }],
			[{ "wait": 500 }, { "c": 735 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 735
			}],
			{}
		],
		[
			1,
			"auto_FR_stellantis.com_67q",
			0,
			"^https?://(www\\.)?cookielaw\\.emea\\.fcagroup\\.com/",
			1,
			[],
			[{ "e": 735 }],
			[{ "v": 735 }],
			[{ "wait": 500 }, { "c": 735 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 735
			}],
			{}
		],
		[
			1,
			"auto_GB_dropbox.com_0_+1",
			0,
			"^https?://(www\\.)?dropbox\\.com/|^https?://(www\\.)?dropbox\\.com/",
			1,
			[],
			[{ "e": 736 }],
			[{ "v": 736 }],
			[{ "c": 736 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_investing.thisismoney.co.uk_0",
			0,
			"^https?://(www\\.)?thisismoney\\.co\\.uk/",
			1,
			[],
			[{ "e": 737 }],
			[{ "v": 737 }],
			[{ "c": 737 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_village-hotels.co.uk_hsa",
			0,
			"^https?://(www\\.)?village-hotels\\.co\\.uk/",
			1,
			[],
			[{ "e": 738 }],
			[{ "v": 738 }],
			[{ "wait": 500 }, { "c": 738 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 738
			}],
			{}
		],
		[
			1,
			"auto_NL_fiat.nl_trv_+1",
			0,
			"^https?://(www\\.)?cookielaw\\.emea\\.fcagroup\\.com/|^https?://(www\\.)?cookielaw\\.emea\\.fcagroup\\.com/",
			1,
			[],
			[{ "e": 735 }],
			[{ "v": 735 }],
			[{ "wait": 500 }, { "c": 735 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 735
			}],
			{}
		],
		[
			1,
			"auto_NL_flitsmeister.nl_ws8",
			0,
			"^https?://(www\\.)?cookies\\.flitsmeister\\.com/",
			1,
			[],
			[{ "e": 739 }],
			[{ "v": 739 }],
			[{ "wait": 500 }, { "c": 739 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 739
			}],
			{}
		],
		[
			1,
			"auto_NL_interpolis.nl_jk9",
			0,
			"^https?://(www\\.)?interpolis\\.nl/",
			1,
			[],
			[{ "e": 740 }],
			[{ "v": 740 }],
			[{ "wait": 500 }, { "c": 740 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 740
			}],
			{}
		],
		[
			1,
			"auto_US_dropbox.com_0_+1",
			0,
			"^https?://(www\\.)?dropbox\\.com/|^https?://(www\\.)?dropbox\\.com/",
			1,
			[],
			[{ "e": 741 }],
			[{ "v": 741 }],
			[{
				"text": "Decline",
				"c": 741
			}],
			[],
			{}
		],
		[
			1,
			"coinbase",
			2,
			"^https://(www|help)\\.coinbase\\.com",
			11,
			[],
			[{ "e": 742 }],
			[{ "v": 742 }],
			[
				{ "k": 743 },
				{
					"all": true,
					"optional": true,
					"k": 744
				},
				{ "k": 745 }
			],
			[{ "eval": "EVAL_COINBASE_0" }],
			{ "intermediate": false }
		],
		[
			1,
			"privacymanager.io",
			2,
			"^https://cmp-consent-tool\\.privacymanager\\.io/",
			1,
			[746, 747],
			[{ "e": 748 }],
			[{ "v": 748 }],
			[{
				"if": { "e": 749 },
				"then": [{ "k": 749 }, { "c": 750 }],
				"else": [
					{ "c": 751 },
					{ "w": 752 },
					{ "w": 753 },
					{
						"all": true,
						"optional": true,
						"k": 754
					},
					{ "k": 753 }
				]
			}],
			[],
			{}
		],
		[
			1,
			"web.de",
			2,
			"^https://([a-z]*\\.)?web\\.de/|^https://([a-z]*\\.)?gmx\\.net/",
			1,
			[],
			[{ "e": 755 }, { "e": 756 }],
			[{ "v": 756 }],
			[{ "c": 756 }],
			[],
			{}
		],
		[
			1,
			"abc",
			2,
			"^https://([a-z0-9-]+\\.)?abc\\.net\\.au/",
			22,
			[],
			[{ "e": 757 }],
			[{ "v": 758 }],
			[{ "c": 759 }],
			[{ "cc": 760 }],
			{}
		],
		[
			1,
			"activobank.pt",
			2,
			"^https://(www\\.)?activobank\\.pt",
			22,
			[761],
			[{ "e": 762 }],
			[{ "v": 763 }],
			[{ "c": 764 }],
			[],
			{}
		],
		[
			1,
			"adultfriendfinder",
			2,
			"^https://(www\\.)?adultfriendfinder\\.com/",
			22,
			[765],
			[{ "e": 766 }],
			[{ "v": 766 }],
			[{ "c": 767 }],
			[{ "eval": "EVAL_ADULTFRIENDFINDER_TEST" }],
			{}
		],
		[
			1,
			"ah.nl",
			0,
			"^https?://(www\\.)?ah\\.nl/",
			10,
			[],
			[{ "e": 768 }],
			[{ "v": 768 }],
			[{ "c": 769 }],
			[],
			{}
		],
		[
			1,
			"alaskaair",
			1,
			"^https://(www\\.)?alaskaair\\.com/",
			22,
			[770],
			[{ "e": 770 }],
			[{ "v": 770 }],
			[{ "h": 770 }],
			[],
			{}
		],
		[
			1,
			"aliexpress",
			2,
			"^https://([a-z]*\\.)?aliexpress\\.com/",
			22,
			[771],
			[{ "e": 772 }],
			[{ "v": 772 }],
			[{
				"if": { "e": 773 },
				"then": [{ "c": 774 }],
				"else": [
					{ "c": 775 },
					{ "w": 776 },
					{
						"all": true,
						"optional": true,
						"k": 777
					},
					{ "k": 778 }
				]
			}],
			[],
			{}
		],
		[
			1,
			"ally",
			1,
			"^https://(www\\.)?ally\\.com/",
			22,
			[779],
			[{ "e": 780 }],
			[{ "v": 780 }],
			[{ "h": 779 }],
			[],
			{}
		],
		[
			1,
			"app.discuss.io",
			2,
			"^https?://app\\.discuss\\.io/",
			10,
			[],
			[{ "e": 513 }],
			[{ "v": 513 }],
			[{ "c": 513 }],
			[{ "cc": 72 }],
			{}
		],
		[
			1,
			"athlinks-com",
			1,
			"^https://(www\\.)?athlinks\\.com/",
			22,
			[781],
			[{ "e": 781 }],
			[{ "v": 782 }],
			[{ "h": 781 }],
			[],
			{}
		],
		[
			1,
			"auto_AU_acetool.com_n0w_+1",
			0,
			"^https?://(www\\.)?acetool\\.com/|^https?://(www\\.)?unclewiener\\.com/",
			10,
			[],
			[{ "e": 783 }],
			[{ "v": 783 }],
			[{ "wait": 500 }, { "c": 783 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 783
			}],
			{}
		],
		[
			1,
			"auto_AU_aelfie.com_chb",
			0,
			"^https?://(www\\.)?aelfie\\.com/",
			10,
			[],
			[{ "e": 784 }],
			[{ "v": 784 }],
			[{ "wait": 500 }, { "c": 784 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 784
			}],
			{}
		],
		[
			1,
			"auto_AU_cam.start.canon_3x5",
			0,
			"^https?://(www\\.)?cam\\.start\\.canon/",
			10,
			[],
			[{ "e": 785 }],
			[{ "v": 785 }],
			[{ "wait": 500 }, { "c": 785 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 785
			}],
			{}
		],
		[
			1,
			"auto_AU_cam.start.canon_z06",
			0,
			"^https?://(www\\.)?cam\\.start\\.canon/",
			10,
			[],
			[{ "e": 786 }],
			[{ "v": 786 }],
			[{ "wait": 500 }, { "c": 786 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 786
			}],
			{}
		],
		[
			1,
			"auto_AU_capitoltrades.com_ak9",
			0,
			"^https?://(www\\.)?capitoltrades\\.com/",
			10,
			[],
			[{ "e": 787 }],
			[{ "v": 787 }],
			[{ "wait": 500 }, { "c": 787 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 787
			}],
			{}
		],
		[
			1,
			"auto_AU_community.dyson.com_lek",
			0,
			"^https?://(www\\.)?community\\.dyson\\.com/",
			10,
			[],
			[{ "e": 788 }],
			[{ "v": 788 }],
			[{ "wait": 500 }, { "c": 788 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 788
			}],
			{}
		],
		[
			1,
			"auto_AU_conference-board.org_dce",
			0,
			"^https?://(www\\.)?conference-board\\.org/",
			10,
			[],
			[{ "e": 789 }],
			[{ "v": 789 }],
			[{ "wait": 500 }, { "c": 789 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 789
			}],
			{}
		],
		[
			1,
			"auto_AU_cutterbuck.com_ko7",
			0,
			"^https?://(www\\.)?cutterbuck\\.com/",
			10,
			[],
			[{ "e": 790 }],
			[{ "v": 790 }],
			[{ "wait": 500 }, { "c": 790 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 790
			}],
			{}
		],
		[
			1,
			"auto_AU_deezer.com_tmf",
			0,
			"^https?://(www\\.)?deezer\\.com/",
			10,
			[],
			[{ "e": 791 }],
			[{ "v": 791 }],
			[{ "wait": 500 }, { "c": 791 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 791
			}],
			{}
		],
		[
			1,
			"auto_AU_docs.portainer.io_hn5",
			0,
			"^https?://(www\\.)?docs\\.portainer\\.io/",
			10,
			[],
			[{ "e": 792 }],
			[{ "v": 792 }],
			[{ "wait": 500 }, { "c": 792 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 792
			}],
			{}
		],
		[
			1,
			"auto_AU_flinders.edu.au_7en_+1",
			0,
			"^https?://(www\\.)?flinders\\.edu\\.au/|^https?://(www\\.)?students\\.flinders\\.edu\\.au/",
			10,
			[],
			[{ "e": 793 }],
			[{ "v": 793 }],
			[{ "c": 793 }],
			[],
			{}
		],
		[
			1,
			"auto_AU_flysaa.com_qsm",
			0,
			"^https?://(www\\.)?flysaa\\.com/",
			10,
			[794],
			[{ "e": 794 }],
			[{ "v": 795 }],
			[{ "c": 796 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 795
			}],
			{}
		],
		[
			1,
			"auto_AU_flysas.com_r1s",
			0,
			"^https?://(www\\.)?flysas\\.com/",
			10,
			[],
			[{ "e": 797 }],
			[{ "v": 797 }],
			[{ "wait": 500 }, { "c": 797 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 797
			}],
			{}
		],
		[
			1,
			"auto_AU_gayseniordating.com_bdr",
			0,
			"^https?://(www\\.)?gayseniordating\\.com/",
			10,
			[],
			[{ "e": 798 }],
			[{ "v": 798 }],
			[{ "wait": 500 }, { "c": 798 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 798
			}],
			{}
		],
		[
			1,
			"auto_AU_goodmoodprints.com_9fy",
			1,
			"^https?://(www\\.)?goodmoodprints\\.com/",
			10,
			[799],
			[{ "e": 799 }],
			[{ "v": 799 }],
			[{ "h": 799 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 799
			}],
			{}
		],
		[
			1,
			"auto_AU_lush.com_bdf",
			0,
			"^https?://(www\\.)?lush\\.com/",
			10,
			[],
			[{ "e": 800 }],
			[{ "v": 800 }],
			[{ "wait": 500 }, { "c": 800 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 800
			}],
			{}
		],
		[
			1,
			"auto_AU_newyork.doverstreetmarket.com_j5q",
			0,
			"^https?://(www\\.)?newyork\\.doverstreetmarket\\.com/",
			10,
			[],
			[{ "e": 801 }],
			[{ "v": 801 }],
			[{ "wait": 500 }, { "c": 801 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 801
			}],
			{}
		],
		[
			1,
			"auto_AU_newyork.doverstreetmarket.com_k2q",
			0,
			"^https?://(www\\.)?newyork\\.doverstreetmarket\\.com/",
			10,
			[],
			[{ "e": 802 }],
			[{ "v": 802 }],
			[{ "wait": 500 }, { "c": 802 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 802
			}],
			{}
		],
		[
			1,
			"auto_AU_niwaki.com_w41",
			0,
			"^https?://(www\\.)?niwaki\\.com/",
			10,
			[],
			[{ "e": 803 }],
			[{ "v": 803 }],
			[{ "wait": 500 }, { "c": 803 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 803
			}],
			{}
		],
		[
			1,
			"auto_AU_pichunter.com_zhf",
			0,
			"^https?://(www\\.)?pichunter\\.com/",
			10,
			[],
			[{ "e": 804 }],
			[{ "v": 804 }],
			[{ "wait": 500 }, { "c": 804 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 804
			}],
			{}
		],
		[
			1,
			"auto_AU_pnp.co.za_9lg",
			0,
			"^https?://(www\\.)?pnp\\.co\\.za/",
			10,
			[],
			[{ "e": 805 }],
			[{ "v": 805 }],
			[{ "wait": 500 }, { "c": 805 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 805
			}],
			{}
		],
		[
			1,
			"auto_AU_publicdomainreview.org_7fg",
			0,
			"^https?://(www\\.)?publicdomainreview\\.org/",
			10,
			[],
			[{ "e": 806 }],
			[{ "v": 806 }],
			[{ "wait": 500 }, { "c": 806 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 806
			}],
			{}
		],
		[
			1,
			"auto_AU_qldnaturistassoc.org_k53",
			0,
			"^https?://(www\\.)?qldnaturistassoc\\.org/",
			10,
			[],
			[{ "e": 807 }],
			[{ "v": 807 }],
			[{ "wait": 500 }, { "c": 807 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 807
			}],
			{}
		],
		[
			1,
			"auto_AU_rki.de_tp5",
			0,
			"^https?://(www\\.)?rki\\.de/",
			10,
			[],
			[{ "e": 808 }],
			[{ "v": 808 }],
			[{ "wait": 500 }, { "c": 808 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 808
			}],
			{}
		],
		[
			1,
			"auto_AU_solesavy.com_8q6",
			0,
			"^https?://(www\\.)?solesavy\\.com/",
			10,
			[],
			[{ "e": 809 }],
			[{ "v": 809 }],
			[{ "wait": 500 }, { "c": 809 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 809
			}],
			{}
		],
		[
			1,
			"auto_AU_staff.flinders.edu.au_cpu",
			0,
			"^https?://(www\\.)?staff\\.flinders\\.edu\\.au/",
			10,
			[],
			[{ "e": 793 }],
			[{ "v": 793 }],
			[{ "wait": 500 }, { "c": 793 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 793
			}],
			{}
		],
		[
			1,
			"auto_AU_telekom.hu_4lu",
			0,
			"^https?://(www\\.)?telekom\\.hu/",
			10,
			[],
			[{ "e": 810 }],
			[{ "v": 810 }],
			[{ "wait": 500 }, { "c": 810 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 810
			}],
			{}
		],
		[
			1,
			"auto_AU_tourismnewbrunswick.ca_bjd",
			0,
			"^https?://(www\\.)?tourismnewbrunswick\\.ca/",
			10,
			[],
			[{ "e": 811 }],
			[{ "v": 811 }],
			[{ "wait": 500 }, { "c": 811 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 811
			}],
			{}
		],
		[
			1,
			"auto_AU_tripcentral.ca_v7v",
			0,
			"^https?://(www\\.)?tripcentral\\.ca/",
			10,
			[],
			[{ "e": 812 }],
			[{ "v": 812 }],
			[{ "wait": 500 }, { "c": 812 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 812
			}],
			{}
		],
		[
			1,
			"auto_AU_u-buy.com.au_rg0",
			0,
			"^https?://(www\\.)?u-buy\\.com\\.au/",
			10,
			[],
			[{ "e": 813 }],
			[{ "v": 813 }],
			[{ "wait": 500 }, { "c": 813 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 813
			}],
			{}
		],
		[
			1,
			"auto_AU_whitepages.co.com_4cs",
			0,
			"^https?://(www\\.)?whitepages\\.co\\.com/",
			10,
			[],
			[{ "e": 814 }],
			[{ "v": 814 }],
			[{ "wait": 500 }, { "c": 814 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 814
			}],
			{}
		],
		[
			1,
			"auto_CA_407etr.com_wrd",
			0,
			"^https?://(www\\.)?407etr\\.com/",
			10,
			[],
			[{ "e": 815 }],
			[{ "v": 815 }],
			[{ "wait": 500 }, { "c": 815 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 815
			}],
			{}
		],
		[
			1,
			"auto_CA_algonquincollege.com_o8v",
			0,
			"^https?://(www\\.)?algonquincollege\\.com/",
			10,
			[],
			[{ "e": 816 }],
			[{ "v": 816 }],
			[{ "wait": 500 }, { "c": 816 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 816
			}],
			{}
		],
		[
			1,
			"auto_CA_arte.tv_7nv",
			0,
			"^https?://(www\\.)?arte\\.tv/",
			10,
			[],
			[{ "e": 817 }],
			[{ "v": 817 }],
			[{ "c": 817 }],
			[],
			{}
		],
		[
			1,
			"auto_CA_babel.hathitrust.org_i6g",
			0,
			"^https?://(www\\.)?babel\\.hathitrust\\.org/",
			10,
			[],
			[{ "e": 818 }],
			[{ "v": 818 }],
			[{ "wait": 500 }, { "c": 818 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 818
			}],
			{}
		],
		[
			1,
			"auto_CA_belairdirect.com_wic",
			0,
			"^https?://(www\\.)?belairdirect\\.com/",
			10,
			[],
			[{ "e": 819 }],
			[{ "v": 819 }],
			[{ "wait": 500 }, { "c": 819 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 819
			}],
			{}
		],
		[
			1,
			"auto_CA_bet365.bet.br_kkx",
			0,
			"^https?://(www\\.)?bet365\\.bet\\.br/",
			10,
			[],
			[{ "e": 820 }],
			[{ "v": 820 }],
			[{ "wait": 500 }, { "c": 820 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 820
			}],
			{}
		],
		[
			1,
			"auto_CA_blackdiamondequipment.com_dwp_+8",
			0,
			"^https?://(www\\.)?blackdiamondequipment\\.com/|^https?://(www\\.)?facetofacegames\\.com/|^https?://(www\\.)?suzyshier\\.com/|^https?://(www\\.)?urban-planet\\.com/|^https?://(www\\.)?eu\\.blackdiamondequipment\\.com/|^https?://(www\\.)?bushwear\\.co\\.uk/|^https?://(www\\.)?directdoors\\.com/|^https?://(www\\.)?keenfootwear\\.com/|^https?://(www\\.)?shop\\.panasonic\\.com/",
			10,
			[],
			[{ "e": 821 }],
			[{ "v": 821 }],
			[{ "wait": 500 }, { "c": 821 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 821
			}],
			{}
		],
		[
			1,
			"auto_CA_brazzers.com_kik",
			0,
			"^https?://(www\\.)?brazzers\\.com/",
			10,
			[],
			[{ "e": 822 }],
			[{ "v": 822 }],
			[{ "wait": 500 }, { "c": 822 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 822
			}],
			{}
		],
		[
			1,
			"auto_CA_busbud.com_0sh",
			0,
			"^https?://(www\\.)?busbud\\.com/",
			10,
			[],
			[{ "e": 823 }],
			[{ "v": 823 }],
			[{ "wait": 500 }, { "c": 823 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 823
			}],
			{}
		],
		[
			1,
			"auto_CA_cegeplimoilou.ca_9tg",
			0,
			"^https?://(www\\.)?cegeplimoilou\\.ca/",
			10,
			[],
			[{ "e": 824 }],
			[{ "v": 824 }],
			[{ "wait": 500 }, { "c": 824 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 824
			}],
			{}
		],
		[
			1,
			"auto_CA_cfo.coop_edx",
			0,
			"^https?://(www\\.)?cfo\\.coop/",
			10,
			[],
			[{ "e": 825 }],
			[{ "v": 825 }],
			[{ "wait": 500 }, { "c": 825 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 825
			}],
			{}
		],
		[
			1,
			"auto_CA_chipolo.net_zo9",
			0,
			"^https?://(www\\.)?chipolo\\.net/",
			10,
			[],
			[{ "e": 826 }],
			[{ "v": 826 }],
			[{ "wait": 500 }, { "c": 826 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 826
			}],
			{}
		],
		[
			1,
			"auto_CA_cmpa-acpm.ca_nja",
			0,
			"^https?://(www\\.)?cmpa-acpm\\.ca/",
			10,
			[],
			[{ "e": 827 }],
			[{ "v": 827 }],
			[{ "c": 827 }],
			[],
			{}
		],
		[
			1,
			"auto_CA_denniskirk.com_ezr",
			0,
			"^https?://(www\\.)?denniskirk\\.com/",
			10,
			[],
			[{ "e": 828 }],
			[{ "v": 828 }],
			[{ "wait": 500 }, { "c": 828 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 828
			}],
			{}
		],
		[
			1,
			"auto_CA_doyondespres.com_4jr",
			0,
			"^https?://(www\\.)?doyondespres\\.com/",
			10,
			[],
			[{ "e": 829 }],
			[{ "v": 829 }],
			[{ "wait": 500 }, { "c": 829 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 829
			}],
			{}
		],
		[
			1,
			"auto_CA_f6s.com_yjw",
			0,
			"^https?://(www\\.)?f6s\\.com/",
			10,
			[],
			[{ "e": 830 }],
			[{ "v": 830 }],
			[{ "wait": 500 }, { "c": 830 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 830
			}],
			{}
		],
		[
			1,
			"auto_CA_fischersports.com_1yr",
			0,
			"^https?://(www\\.)?fischersports\\.com/",
			10,
			[],
			[{ "e": 831 }],
			[{ "v": 831 }],
			[{ "wait": 500 }, { "c": 831 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 831
			}],
			{}
		],
		[
			1,
			"auto_CA_golfavenue.ca_ero_+1",
			0,
			"^https?://(www\\.)?golfavenue\\.ca/|^https?://(www\\.)?golfbidder\\.co\\.uk/",
			10,
			[],
			[{ "e": 832 }],
			[{ "v": 832 }],
			[{ "wait": 500 }, { "c": 832 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 832
			}],
			{}
		],
		[
			1,
			"auto_CA_golftown.com_zcn",
			0,
			"^https?://(www\\.)?golftown\\.com/",
			10,
			[],
			[{ "e": 833 }],
			[{ "v": 833 }],
			[{ "wait": 500 }, { "c": 833 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 833
			}],
			{}
		],
		[
			1,
			"auto_CA_intact.ca_gax",
			0,
			"^https?://(www\\.)?intact\\.ca/",
			10,
			[],
			[{ "e": 834 }],
			[{ "v": 834 }],
			[{ "wait": 500 }, { "c": 834 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 834
			}],
			{}
		],
		[
			1,
			"auto_CA_lethpolytech.ca_1r1",
			0,
			"^https?://(www\\.)?lethpolytech\\.ca/",
			10,
			[],
			[{ "e": 835 }],
			[{ "v": 835 }],
			[{ "wait": 500 }, { "c": 835 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 835
			}],
			{}
		],
		[
			1,
			"auto_CA_libgen.help_7s8",
			0,
			"^https?://(www\\.)?libgen\\.help/",
			10,
			[],
			[{ "e": 836 }],
			[{ "v": 836 }],
			[{ "wait": 500 }, { "c": 836 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 836
			}],
			{}
		],
		[
			1,
			"auto_CA_nationalarchives.gov.uk_vgx",
			0,
			"^https?://(www\\.)?webarchive\\.nationalarchives\\.gov\\.uk/",
			10,
			[],
			[{ "e": 837 }],
			[{ "v": 837 }],
			[{ "wait": 500 }, { "c": 837 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 837
			}],
			{}
		],
		[
			1,
			"auto_CA_papajohns.ca_n5j",
			0,
			"^https?://(www\\.)?papajohns\\.ca/",
			10,
			[],
			[{ "e": 838 }],
			[{ "v": 838 }],
			[{ "wait": 500 }, { "c": 838 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 838
			}],
			{}
		],
		[
			1,
			"auto_CA_pcsupport.lenovo.com_dxi",
			0,
			"^https?://(www\\.)?pcsupport\\.lenovo\\.com/",
			10,
			[],
			[{ "e": 839 }],
			[{ "v": 839 }],
			[{ "wait": 500 }, { "c": 839 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 839
			}],
			{}
		],
		[
			1,
			"auto_CA_plex.tv_ee4_+1",
			0,
			"^https?://(www\\.)?plex\\.tv/|^https?://(www\\.)?support\\.plex\\.tv/",
			10,
			[],
			[{ "e": 840 }],
			[{ "v": 840 }],
			[{ "wait": 500 }, { "c": 840 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 840
			}],
			{}
		],
		[
			1,
			"auto_CA_promessedefleurs.com_noy_+1",
			0,
			"^https?://(www\\.)?promessedefleurs\\.com/|^https?://(www\\.)?meillandrichardier\\.com/",
			10,
			[],
			[{ "e": 841 }],
			[{ "v": 841 }],
			[{ "wait": 500 }, { "c": 841 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 841
			}],
			{}
		],
		[
			1,
			"auto_CA_queerty.com_fb8_+1",
			0,
			"^https?://(www\\.)?queerty\\.com/|^https?://(www\\.)?pestor\\.nl/",
			10,
			[],
			[{ "e": 842 }],
			[{ "v": 842 }],
			[{ "wait": 500 }, { "c": 842 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 842
			}],
			{}
		],
		[
			1,
			"auto_CA_remitly.com_mm4",
			0,
			"^https?://(www\\.)?remitly\\.com/",
			10,
			[],
			[{ "e": 843 }],
			[{ "v": 843 }],
			[{ "wait": 500 }, { "c": 843 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 843
			}],
			{}
		],
		[
			1,
			"auto_CA_reviewed.com_w2k",
			0,
			"^https?://(www\\.)?reviewed\\.com/",
			10,
			[],
			[{ "e": 844 }],
			[{ "v": 844 }],
			[{ "wait": 500 }, { "c": 844 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 844
			}],
			{}
		],
		[
			1,
			"auto_CA_sheetmusicplus.com_b7d",
			0,
			"^https?://(www\\.)?sheetmusicplus\\.com/",
			10,
			[],
			[{ "e": 845 }],
			[{ "v": 845 }],
			[{ "wait": 500 }, { "c": 845 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 845
			}],
			{}
		],
		[
			1,
			"auto_CA_skipthedishes.com_1sg",
			0,
			"^https?://(www\\.)?skipthedishes\\.com/",
			10,
			[],
			[{ "e": 846 }],
			[{ "v": 846 }],
			[{ "c": 846 }],
			[],
			{}
		],
		[
			1,
			"auto_CA_streamingthe.net_v8e",
			0,
			"^https?://(www\\.)?streamingthe\\.net/",
			10,
			[],
			[{ "e": 847 }],
			[{ "v": 847 }],
			[{ "wait": 500 }, { "c": 847 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 847
			}],
			{}
		],
		[
			1,
			"auto_CA_ticketsource.com_kiv",
			0,
			"^https?://(www\\.)?ticketsource\\.com/",
			10,
			[],
			[{ "e": 848 }],
			[{ "v": 848 }],
			[{ "wait": 500 }, { "c": 848 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 848
			}],
			{}
		],
		[
			1,
			"auto_CA_ubereats.com_4nx",
			0,
			"^https?://(www\\.)?ubereats\\.com/",
			10,
			[],
			[{ "e": 849 }],
			[{ "v": 849 }],
			[{ "wait": 500 }, { "c": 849 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 849
			}],
			{}
		],
		[
			1,
			"auto_CA_wealthsimple.com_kgz",
			0,
			"^https?://(www\\.)?wealthsimple\\.com/",
			10,
			[],
			[{ "e": 850 }],
			[{ "v": 850 }],
			[{ "wait": 500 }, { "c": 850 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 850
			}],
			{}
		],
		[
			1,
			"auto_CA_xplore.ca_wwx",
			0,
			"^https?://(www\\.)?xplore\\.ca/",
			10,
			[],
			[{ "e": 851 }],
			[{ "v": 851 }],
			[{ "wait": 500 }, { "c": 851 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 851
			}],
			{}
		],
		[
			1,
			"auto_CH_3djake.ch_6k2_+3",
			0,
			"^https?://(www\\.)?3djake\\.ch/|^https?://(www\\.)?ecco-verde\\.ch/|^https?://(www\\.)?vitalabo\\.ch/|^https?://(www\\.)?3djake\\.de/",
			10,
			[],
			[{ "e": 852 }],
			[{ "v": 852 }],
			[{ "wait": 500 }, { "c": 852 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 852
			}],
			{}
		],
		[
			1,
			"auto_CH_airbnb.de_83z",
			0,
			"^https?://(www\\.)?airbnb\\.de/",
			10,
			[],
			[{ "e": 853 }],
			[{ "v": 853 }],
			[{ "wait": 500 }, { "c": 853 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 853
			}],
			{}
		],
		[
			1,
			"auto_CH_alpenvereinaktiv.com_ms8_+1",
			0,
			"^https?://(www\\.)?alpenvereinaktiv\\.com/|^https?://(www\\.)?maps\\.viamala\\.ch/",
			10,
			[],
			[{ "e": 854 }],
			[{ "v": 854 }],
			[{ "wait": 500 }, { "c": 854 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 854
			}],
			{}
		],
		[
			1,
			"auto_CH_android.bestsecret.com_m12_+2",
			0,
			"^https?://(www\\.)?android\\.bestsecret\\.com/|^https?://(www\\.)?bestsecret\\.com/|^https?://(www\\.)?orders\\.bestsecret\\.com/",
			10,
			[],
			[{ "e": 855 }],
			[{ "v": 855 }],
			[{ "wait": 500 }, { "c": 855 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 855
			}],
			{}
		],
		[
			1,
			"auto_CH_aoc.com_dib",
			0,
			"^https?://(www\\.)?aoc\\.com/",
			10,
			[],
			[{ "e": 856 }],
			[{ "v": 856 }],
			[{ "wait": 500 }, { "c": 856 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 856
			}],
			{}
		],
		[
			1,
			"auto_CH_arte.tv_ln7",
			0,
			"^https?://(www\\.)?arte\\.tv/",
			10,
			[],
			[{ "e": 857 }],
			[{ "v": 857 }],
			[{ "wait": 500 }, { "c": 857 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 857
			}],
			{}
		],
		[
			1,
			"auto_CH_arttv.ch_2lm",
			0,
			"^https?://(www\\.)?arttv\\.ch/",
			10,
			[],
			[{ "e": 858 }],
			[{ "v": 858 }],
			[{ "wait": 500 }, { "c": 858 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 858
			}],
			{}
		],
		[
			1,
			"auto_CH_ascona.il-centro.ch_59k",
			0,
			"^https?://(www\\.)?ascona\\.il-centro\\.ch/",
			10,
			[],
			[{ "e": 859 }],
			[{ "v": 859 }],
			[{ "wait": 500 }, { "c": 859 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 859
			}],
			{}
		],
		[
			1,
			"auto_CH_astag.ch_1if",
			0,
			"^https?://(www\\.)?astag\\.ch/",
			10,
			[],
			[{ "e": 860 }],
			[{ "v": 860 }],
			[{ "wait": 500 }, { "c": 860 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 860
			}],
			{}
		],
		[
			1,
			"auto_CH_auto.swiss_z35",
			0,
			"^https?://(www\\.)?auto\\.swiss/",
			10,
			[],
			[{ "e": 861 }],
			[{ "v": 861 }],
			[{ "wait": 500 }, { "c": 861 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 861
			}],
			{}
		],
		[
			1,
			"auto_CH_autodoc.de_krd",
			0,
			"^https?://(www\\.)?autodoc\\.de/",
			10,
			[],
			[{ "e": 862 }],
			[{ "v": 862 }],
			[{ "wait": 500 }, { "c": 862 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 862
			}],
			{}
		],
		[
			1,
			"auto_CH_bandlab.com_jul",
			0,
			"^https?://(www\\.)?bandlab\\.com/",
			10,
			[],
			[{ "e": 863 }],
			[{ "v": 863 }],
			[{ "wait": 500 }, { "c": 863 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 863
			}],
			{}
		],
		[
			1,
			"auto_CH_bitbox.swiss_w0n",
			0,
			"^https?://(www\\.)?bitbox\\.swiss/",
			10,
			[],
			[{ "e": 864 }],
			[{ "v": 864 }],
			[{ "wait": 500 }, { "c": 864 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 864
			}],
			{}
		],
		[
			1,
			"auto_CH_blogdumoderateur.com_8p6",
			0,
			"^https?://(www\\.)?blogdumoderateur\\.com/",
			10,
			[],
			[{ "e": 865 }],
			[{ "v": 865 }],
			[{ "wait": 500 }, { "c": 865 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 865
			}],
			{}
		],
		[
			1,
			"auto_CH_blutspendezurich.ch_s0f",
			0,
			"^https?://(www\\.)?blutspendezurich\\.ch/",
			10,
			[],
			[{ "e": 866 }],
			[{ "v": 866 }],
			[{ "wait": 500 }, { "c": 866 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 866
			}],
			{}
		],
		[
			1,
			"auto_CH_cardmarket.com_15t",
			0,
			"^https?://(www\\.)?cardmarket\\.com/",
			10,
			[],
			[{ "e": 867 }],
			[{ "v": 867 }],
			[{ "wait": 500 }, { "c": 867 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 867
			}],
			{}
		],
		[
			1,
			"auto_CH_ch.rotho.com_h99_+2",
			0,
			"^https?://(www\\.)?ch\\.rotho\\.com/|^https?://(www\\.)?reclam\\.de/|^https?://(www\\.)?dacianer\\.de/",
			10,
			[],
			[{ "e": 868 }],
			[{ "v": 868 }],
			[{ "wait": 500 }, { "c": 868 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 868
			}],
			{}
		],
		[
			1,
			"auto_CH_chambres-hotes.fr_hay",
			0,
			"^https?://(www\\.)?chambres-hotes\\.fr/",
			10,
			[],
			[{ "e": 869 }],
			[{ "v": 869 }],
			[{ "wait": 500 }, { "c": 869 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 869
			}],
			{}
		],
		[
			1,
			"auto_CH_chrono24.ch_dve_+1",
			0,
			"^https?://(www\\.)?chrono24\\.ch/|^https?://(www\\.)?chrono24\\.de/",
			10,
			[],
			[{ "e": 870 }],
			[{ "v": 870 }],
			[{ "wait": 500 }, { "c": 870 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 870
			}],
			{}
		],
		[
			1,
			"auto_CH_chrono24.com_lj5_+1",
			0,
			"^https?://(www\\.)?chrono24\\.com/|^https?://(www\\.)?chrono24\\.co\\.uk/",
			10,
			[],
			[{ "e": 870 }],
			[{ "v": 870 }],
			[{ "wait": 500 }, { "c": 870 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 870
			}],
			{}
		],
		[
			1,
			"auto_CH_coolblue.de_egw",
			0,
			"^https?://(www\\.)?coolblue\\.de/",
			10,
			[],
			[{ "e": 871 }],
			[{ "v": 871 }],
			[{ "wait": 500 }, { "c": 871 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 871
			}],
			{}
		],
		[
			1,
			"auto_CH_ersatzteil-shop24.de_mh5_+1",
			0,
			"^https?://(www\\.)?ersatzteil-shop24\\.de/|^https?://(www\\.)?der-rasenmaeher\\.de/",
			10,
			[],
			[{ "e": 872 }],
			[{ "v": 872 }],
			[{ "wait": 500 }, { "c": 872 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 872
			}],
			{}
		],
		[
			1,
			"auto_CH_euroairport.com_3yl_+4",
			0,
			"^https?://(www\\.)?euroairport\\.com/|^https?://(www\\.)?carnavalet\\.paris\\.fr/|^https?://(www\\.)?ifrap\\.org/|^https?://(www\\.)?mam\\.paris\\.fr/|^https?://(www\\.)?petitpalais\\.paris\\.fr/",
			10,
			[],
			[{ "e": 873 }],
			[{ "v": 873 }],
			[{ "wait": 500 }, { "c": 873 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 873
			}],
			{}
		],
		[
			1,
			"auto_CH_europarl.europa.eu_y2d",
			0,
			"^https?://(www\\.)?europarl\\.europa\\.eu/",
			10,
			[],
			[{ "e": 874 }],
			[{ "v": 874 }],
			[{ "wait": 500 }, { "c": 874 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 874
			}],
			{}
		],
		[
			1,
			"auto_CH_feldschloesschen.ch_4mc",
			0,
			"^https?://(www\\.)?feldschloesschen\\.ch/",
			10,
			[],
			[{ "e": 875 }],
			[{ "v": 875 }],
			[{ "wait": 500 }, { "c": 875 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 875
			}],
			{}
		],
		[
			1,
			"auto_CH_fhgr.ch_9nr",
			0,
			"^https?://(www\\.)?fhgr\\.ch/",
			10,
			[],
			[{ "e": 876 }],
			[{ "v": 876 }],
			[{ "wait": 500 }, { "c": 876 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 876
			}],
			{}
		],
		[
			1,
			"auto_CH_fondationbeyeler.ch_o13",
			0,
			"^https?://(www\\.)?fondationbeyeler\\.ch/",
			10,
			[],
			[{ "e": 868 }],
			[{ "v": 868 }],
			[{ "wait": 500 }, { "c": 868 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 868
			}],
			{}
		],
		[
			1,
			"auto_CH_fontis-shop.ch_ksd",
			0,
			"^https?://(www\\.)?fontis-shop\\.ch/",
			10,
			[],
			[{ "e": 877 }],
			[{ "v": 877 }],
			[{ "wait": 500 }, { "c": 877 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 877
			}],
			{}
		],
		[
			1,
			"auto_CH_frankenspalter.ch_jo0",
			0,
			"^https?://(www\\.)?frankenspalter\\.ch/",
			10,
			[],
			[{ "e": 878 }],
			[{ "v": 878 }],
			[{ "wait": 500 }, { "c": 878 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 878
			}],
			{}
		],
		[
			1,
			"auto_CH_frankenspalter.ch_l2w",
			0,
			"^https?://(www\\.)?frankenspalter\\.ch/",
			10,
			[],
			[{ "e": 879 }],
			[{ "v": 879 }],
			[{ "wait": 500 }, { "c": 879 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 879
			}],
			{}
		],
		[
			1,
			"auto_CH_fruugoschweiz.com_6k8",
			0,
			"^https?://(www\\.)?fruugoschweiz\\.com/",
			10,
			[],
			[{ "e": 880 }],
			[{ "v": 880 }],
			[{ "wait": 500 }, { "c": 880 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 880
			}],
			{}
		],
		[
			1,
			"auto_CH_gabrielweinberg.com_56i",
			0,
			"^https?://(www\\.)?gabrielweinberg\\.com/",
			10,
			[],
			[{ "e": 881 }],
			[{ "v": 881 }],
			[{ "wait": 500 }, { "c": 881 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 881
			}],
			{}
		],
		[
			1,
			"auto_CH_galaxus.de_sqv_+1",
			0,
			"^https?://(www\\.)?galaxus\\.de/|^https?://(www\\.)?galaxus\\.fr/",
			10,
			[],
			[{ "e": 882 }],
			[{ "v": 882 }],
			[{ "wait": 500 }, { "c": 882 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 882
			}],
			{}
		],
		[
			1,
			"auto_CH_gstaadmenuhinfestival.ch_vp3",
			0,
			"^https?://(www\\.)?gstaadmenuhinfestival\\.ch/",
			10,
			[],
			[{ "e": 883 }],
			[{ "v": 883 }],
			[{ "wait": 500 }, { "c": 883 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 883
			}],
			{}
		],
		[
			1,
			"auto_CH_haushaltstipps.com_9kx_+1",
			0,
			"^https?://(www\\.)?haushaltstipps\\.com/|^https?://(www\\.)?alltours\\.de/",
			10,
			[],
			[{ "e": 868 }],
			[{ "v": 868 }],
			[{ "wait": 500 }, { "c": 868 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 868
			}],
			{}
		],
		[
			1,
			"auto_CH_independentxpress.de_0qj",
			0,
			"^https?://(www\\.)?independentxpress\\.de/",
			10,
			[],
			[{ "e": 884 }],
			[{ "v": 884 }],
			[{ "wait": 500 }, { "c": 884 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 884
			}],
			{}
		],
		[
			1,
			"auto_CH_instant-gaming.com_8j3",
			0,
			"^https?://(www\\.)?instant-gaming\\.com/",
			10,
			[],
			[{ "e": 885 }],
			[{ "v": 885 }],
			[{ "wait": 500 }, { "c": 885 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 885
			}],
			{}
		],
		[
			1,
			"auto_CH_kinopoisk.ru_w3g_+14",
			0,
			"^https?://(www\\.)?kinopoisk\\.ru/|^https?://(www\\.)?market\\.yandex\\.ru/|^https?://(www\\.)?translate\\.yandex\\.com/|^https?://(www\\.)?yandex\\.com\\.tr/|^https?://(www\\.)?ya\\.ru/|^https?://(www\\.)?yandex\\.com/|^https?://(www\\.)?translate\\.yandex\\.ru/|^https?://(www\\.)?yandex\\.by/|^https?://(www\\.)?local\\.yandex\\.com/|^https?://(www\\.)?sso\\.passport\\.yandex\\.ru/|^https?://(www\\.)?360\\.yandex\\.ru/|^https?://(www\\.)?360\\.yandex\\.com/|^https?://(www\\.)?music\\.yandex\\.ru/|^https?://(www\\.)?hd\\.kinopoisk\\.ru/|^https?://(www\\.)?wap\\.yandex\\.com/",
			10,
			[],
			[{ "e": 886 }],
			[{ "v": 886 }],
			[{ "wait": 500 }, { "c": 886 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 886
			}],
			{}
		],
		[
			1,
			"auto_CH_kobo.com_p4l",
			0,
			"^https?://(www\\.)?kobo\\.com/",
			10,
			[],
			[{ "e": 887 }],
			[{ "v": 887 }],
			[{ "wait": 500 }, { "c": 887 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 887
			}],
			{}
		],
		[
			1,
			"auto_CH_labanquepostale.fr_o6f_+1",
			0,
			"^https?://(www\\.)?labanquepostale\\.fr/|^https?://(www\\.)?labanquepostale\\.com/",
			10,
			[],
			[{ "e": 888 }],
			[{ "v": 888 }],
			[{ "wait": 500 }, { "c": 888 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 888
			}],
			{}
		],
		[
			1,
			"auto_CH_laposte.fr_ctu_+2",
			0,
			"^https?://(www\\.)?laposte\\.fr/|^https?://(www\\.)?localiser\\.laposte\\.fr/|^https?://(www\\.)?aide\\.laposte\\.fr/",
			10,
			[],
			[{ "e": 889 }],
			[{ "v": 889 }],
			[{ "wait": 500 }, { "c": 889 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 889
			}],
			{}
		],
		[
			1,
			"auto_CH_larian.com_kp5",
			0,
			"^https?://(www\\.)?larian\\.com/",
			10,
			[],
			[{ "e": 890 }],
			[{ "v": 890 }],
			[{ "wait": 500 }, { "c": 890 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 890
			}],
			{}
		],
		[
			1,
			"auto_CH_louis-moto.ch_5yk",
			0,
			"^https?://(www\\.)?louis-moto\\.ch/",
			10,
			[],
			[{ "e": 891 }],
			[{ "v": 891 }],
			[{ "wait": 500 }, { "c": 891 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 891
			}],
			{}
		],
		[
			1,
			"auto_CH_manufactum.ch_ag0_+1",
			0,
			"^https?://(www\\.)?manufactum\\.ch/|^https?://(www\\.)?manufactum\\.de/",
			10,
			[],
			[{ "e": 892 }],
			[{ "v": 892 }],
			[{ "wait": 500 }, { "c": 892 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 892
			}],
			{}
		],
		[
			1,
			"auto_CH_maps.engadin.ch_m40_+3",
			0,
			"^https?://(www\\.)?maps\\.engadin\\.ch/|^https?://(www\\.)?outdoor\\.glarnerland\\.ch/|^https?://(www\\.)?altenberg\\.de/|^https?://(www\\.)?tourenplaner-rheinland-pfalz\\.de/",
			10,
			[],
			[{ "e": 854 }],
			[{ "v": 854 }],
			[{ "wait": 500 }, { "c": 854 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 854
			}],
			{}
		],
		[
			1,
			"auto_CH_mein-kraeuterkeller.de_zjh",
			0,
			"^https?://(www\\.)?mein-kraeuterkeller\\.de/",
			10,
			[],
			[{ "e": 893 }],
			[{ "v": 893 }],
			[{ "wait": 500 }, { "c": 893 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 893
			}],
			{}
		],
		[
			1,
			"auto_CH_meintiptopf.ch_84l",
			0,
			"^https?://(www\\.)?meintiptopf\\.ch/",
			10,
			[],
			[{ "e": 894 }],
			[{ "v": 894 }],
			[{ "wait": 500 }, { "c": 894 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 894
			}],
			{}
		],
		[
			1,
			"auto_CH_mio.se_hd0",
			0,
			"^https?://(www\\.)?mio\\.se/",
			10,
			[],
			[{ "e": 895 }],
			[{ "v": 895 }],
			[{ "wait": 500 }, { "c": 895 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 895
			}],
			{}
		],
		[
			1,
			"auto_CH_moebel24.ch_fck_+1",
			0,
			"^https?://(www\\.)?moebel24\\.ch/|^https?://(www\\.)?moebel\\.de/",
			10,
			[],
			[{ "e": 896 }],
			[{ "v": 896 }],
			[{ "wait": 500 }, { "c": 896 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 896
			}],
			{}
		],
		[
			1,
			"auto_CH_mrporter.com_0nl_+1",
			0,
			"^https?://(www\\.)?mrporter\\.com/|^https?://(www\\.)?net-a-porter\\.com/",
			10,
			[],
			[{ "e": 897 }],
			[{ "v": 897 }],
			[{ "wait": 500 }, { "c": 897 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 897
			}],
			{}
		],
		[
			1,
			"auto_CH_platform.openai.com_g5y",
			0,
			"^https?://(www\\.)?platform\\.openai\\.com/",
			10,
			[],
			[{ "e": 898 }],
			[{ "v": 898 }],
			[{ "wait": 500 }, { "c": 898 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 898
			}],
			{}
		],
		[
			1,
			"auto_CH_pmphotomedia.ch_ume",
			0,
			"^https?://(www\\.)?pmphotomedia\\.ch/",
			10,
			[],
			[{ "e": 899 }],
			[{ "v": 899 }],
			[{ "wait": 500 }, { "c": 899 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 899
			}],
			{}
		],
		[
			1,
			"auto_CH_radio1.ch_1v2",
			0,
			"^https?://(www\\.)?radio1\\.ch/",
			10,
			[],
			[{ "e": 900 }],
			[{ "v": 900 }],
			[{ "wait": 500 }, { "c": 900 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 900
			}],
			{}
		],
		[
			1,
			"auto_CH_renens.ch_2uc",
			0,
			"^https?://(www\\.)?renens\\.ch/",
			10,
			[],
			[{ "e": 901 }],
			[{ "v": 901 }],
			[{ "wait": 500 }, { "c": 901 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 901
			}],
			{}
		],
		[
			1,
			"auto_CH_sac-uto.ch_3gu",
			0,
			"^https?://(www\\.)?sac-uto\\.ch/",
			10,
			[],
			[{ "e": 902 }],
			[{ "v": 902 }],
			[{ "wait": 500 }, { "c": 902 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 902
			}],
			{}
		],
		[
			1,
			"auto_CH_seetickets.com_4b2",
			0,
			"^https?://(www\\.)?seetickets\\.com/",
			10,
			[],
			[{ "e": 903 }],
			[{ "v": 903 }],
			[{ "wait": 500 }, { "c": 903 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 903
			}],
			{}
		],
		[
			1,
			"auto_CH_sparkasse.de_lwr",
			0,
			"^https?://(www\\.)?sparkasse\\.de/",
			10,
			[],
			[{ "e": 904 }],
			[{ "v": 904 }],
			[{ "wait": 500 }, { "c": 904 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 904
			}],
			{}
		],
		[
			1,
			"auto_CH_sva-bl.ch_h2k_+1",
			0,
			"^https?://(www\\.)?sva-bl\\.ch/|^https?://(www\\.)?uni-bremen\\.de/",
			10,
			[],
			[{ "e": 905 }],
			[{ "v": 905 }],
			[{ "wait": 500 }, { "c": 905 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 905
			}],
			{}
		],
		[
			1,
			"auto_CH_svtplay.se_d39",
			0,
			"^https?://(www\\.)?svtplay\\.se/",
			10,
			[],
			[{ "e": 906 }],
			[{ "v": 906 }],
			[{ "wait": 500 }, { "c": 906 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 906
			}],
			{}
		],
		[
			1,
			"auto_CH_transn.ch_ygb",
			0,
			"^https?://(www\\.)?transn\\.ch/",
			10,
			[],
			[{ "e": 907 }],
			[{ "v": 907 }],
			[{ "wait": 500 }, { "c": 907 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 907
			}],
			{}
		],
		[
			1,
			"auto_CH_ubs-kidscup.ch_5oc",
			0,
			"^https?://(www\\.)?ubs-kidscup\\.ch/",
			10,
			[],
			[{ "e": 908 }],
			[{ "v": 908 }],
			[{ "wait": 500 }, { "c": 908 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 908
			}],
			{}
		],
		[
			1,
			"auto_CH_velofactory.ch_tpq",
			0,
			"^https?://(www\\.)?velofactory\\.ch/",
			10,
			[],
			[{ "e": 909 }],
			[{ "v": 909 }],
			[{ "wait": 500 }, { "c": 909 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 909
			}],
			{}
		],
		[
			1,
			"auto_DE_116117-termine.de_nwg",
			0,
			"^https?://(www\\.)?116117-termine\\.de/",
			10,
			[],
			[{ "e": 910 }],
			[{ "v": 910 }],
			[{ "wait": 500 }, { "c": 910 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 910
			}],
			{}
		],
		[
			1,
			"auto_DE_6relax.de_na9",
			0,
			"^https?://(www\\.)?6relax\\.de/",
			10,
			[],
			[{ "e": 911 }],
			[{ "v": 911 }],
			[{ "wait": 500 }, { "c": 911 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 911
			}],
			{}
		],
		[
			1,
			"auto_DE_accio.com_kdu",
			0,
			"^https?://(www\\.)?accio\\.com/",
			10,
			[],
			[{ "e": 912 }],
			[{ "v": 912 }],
			[{ "wait": 500 }, { "c": 912 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 912
			}],
			{}
		],
		[
			1,
			"auto_DE_aerztekammer-bw.de_rse",
			0,
			"^https?://(www\\.)?aerztekammer-bw\\.de/",
			10,
			[],
			[{ "e": 913 }],
			[{ "v": 913 }],
			[{ "wait": 500 }, { "c": 913 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 913
			}],
			{}
		],
		[
			1,
			"auto_DE_afd.de_tad",
			0,
			"^https?://(www\\.)?afd\\.de/",
			10,
			[],
			[{ "e": 914 }],
			[{ "v": 914 }],
			[{ "wait": 500 }, { "c": 914 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 914
			}],
			{}
		],
		[
			1,
			"auto_DE_aknw.de_wcz_+1",
			0,
			"^https?://(www\\.)?aknw\\.de/|^https?://(www\\.)?regioentsorgung\\.de/",
			10,
			[],
			[{ "e": 915 }],
			[{ "v": 915 }],
			[{ "wait": 500 }, { "c": 915 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 915
			}],
			{}
		],
		[
			1,
			"auto_DE_all-inkl.com_toh",
			0,
			"^https?://(www\\.)?all-inkl\\.com/",
			10,
			[],
			[{ "e": 916 }],
			[{ "v": 916 }],
			[{ "wait": 500 }, { "c": 916 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 916
			}],
			{}
		],
		[
			1,
			"auto_DE_almenrausch.at_c6z",
			0,
			"^https?://(www\\.)?almenrausch\\.at/",
			10,
			[],
			[{ "e": 917 }],
			[{ "v": 917 }],
			[{ "wait": 500 }, { "c": 917 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 917
			}],
			{}
		],
		[
			1,
			"auto_DE_alza.de_iq3",
			0,
			"^https?://(www\\.)?alza\\.de/",
			10,
			[],
			[{ "e": 918 }],
			[{ "v": 918 }],
			[{ "wait": 500 }, { "c": 918 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 918
			}],
			{}
		],
		[
			1,
			"auto_DE_ancestry.com_k1k",
			0,
			"^https?://(www\\.)?ancestry\\.com/",
			10,
			[],
			[{ "e": 919 }],
			[{ "v": 919 }],
			[{ "wait": 500 }, { "c": 919 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 919
			}],
			{}
		],
		[
			1,
			"auto_DE_apozilla.de_d1h",
			0,
			"^https?://(www\\.)?apozilla\\.de/",
			10,
			[],
			[{ "e": 920 }],
			[{ "v": 920 }],
			[{ "wait": 500 }, { "c": 920 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 920
			}],
			{}
		],
		[
			1,
			"auto_DE_ardplus.de_ue3",
			0,
			"^https?://(www\\.)?ardplus\\.de/",
			10,
			[],
			[{ "e": 921 }],
			[{ "v": 921 }],
			[{ "wait": 500 }, { "c": 921 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 921
			}],
			{}
		],
		[
			1,
			"auto_DE_axa.de_vyk",
			0,
			"^https?://(www\\.)?axa\\.de/",
			10,
			[],
			[{ "e": 922 }],
			[{ "v": 922 }],
			[{ "wait": 500 }, { "c": 922 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 922
			}],
			{}
		],
		[
			1,
			"auto_DE_backmarket.de_dzf",
			0,
			"^https?://(www\\.)?backmarket\\.de/",
			10,
			[],
			[{ "e": 923 }],
			[{ "v": 923 }],
			[{ "wait": 500 }, { "c": 923 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 923
			}],
			{}
		],
		[
			1,
			"auto_DE_bbbank.de_dcf_+15",
			0,
			"^https?://(www\\.)?bbbank\\.de/|^https?://(www\\.)?diebank\\.de/|^https?://(www\\.)?genobroker\\.de/|^https?://(www\\.)?ligabank\\.de/|^https?://(www\\.)?pax-bkc\\.de/|^https?://(www\\.)?psd-berlin-brandenburg\\.de/|^https?://(www\\.)?psd-nuernberg\\.de/|^https?://(www\\.)?sparda-bank-hamburg\\.de/|^https?://(www\\.)?sparda-h\\.de/|^https?://(www\\.)?sparda-n\\.de/|^https?://(www\\.)?v-mn\\.de/|^https?://(www\\.)?vr-bayernmitte\\.de/|^https?://(www\\.)?vrbank-brs\\.de/|^https?://(www\\.)?vrbank-eg\\.de/|^https?://(www\\.)?vrbank-lb\\.de/|^https?://(www\\.)?wvb\\.de/",
			10,
			[],
			[{ "e": 924 }],
			[{ "v": 924 }],
			[{ "wait": 500 }, { "c": 924 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 924
			}],
			{}
		],
		[
			1,
			"auto_DE_biunsinnorden.de_kwa_+1",
			0,
			"^https?://(www\\.)?biunsinnorden\\.de/|^https?://(www\\.)?livegigs\\.de/",
			10,
			[],
			[{ "e": 925 }],
			[{ "v": 925 }],
			[{ "wait": 500 }, { "c": 925 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 925
			}],
			{}
		],
		[
			1,
			"auto_DE_bmz.de_nss",
			0,
			"^https?://(www\\.)?bmz\\.de/",
			10,
			[],
			[{ "e": 926 }],
			[{ "v": 926 }],
			[{ "wait": 500 }, { "c": 926 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 926
			}],
			{}
		],
		[
			1,
			"auto_DE_buerklin.com_bya",
			0,
			"^https?://(www\\.)?buerklin\\.com/",
			10,
			[],
			[{ "e": 868 }],
			[{ "v": 868 }],
			[{ "wait": 500 }, { "c": 868 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 868
			}],
			{}
		],
		[
			1,
			"auto_DE_bundeswehrkarriere.de_g9g",
			0,
			"^https?://(www\\.)?bundeswehrkarriere\\.de/",
			10,
			[],
			[{ "e": 927 }],
			[{ "v": 927 }],
			[{ "wait": 500 }, { "c": 927 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 927
			}],
			{}
		],
		[
			1,
			"auto_DE_byak.de_dcj",
			0,
			"^https?://(www\\.)?byak\\.de/",
			10,
			[],
			[{ "e": 928 }],
			[{ "v": 928 }],
			[{ "wait": 500 }, { "c": 928 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 928
			}],
			{}
		],
		[
			1,
			"auto_DE_byte.fm_83l",
			0,
			"^https?://(www\\.)?byte\\.fm/",
			10,
			[],
			[{ "e": 929 }],
			[{ "v": 929 }],
			[{ "wait": 500 }, { "c": 929 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 929
			}],
			{}
		],
		[
			1,
			"auto_DE_camping-outdoorshop.de_oo4_+1",
			0,
			"^https?://(www\\.)?camping-outdoorshop\\.de/|^https?://(www\\.)?elektro-wandelt\\.de/",
			10,
			[],
			[{ "e": 917 }],
			[{ "v": 917 }],
			[{ "wait": 500 }, { "c": 917 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 917
			}],
			{}
		],
		[
			1,
			"auto_DE_club.auto-doc.at_6xj",
			0,
			"^https?://(www\\.)?club\\.auto-doc\\.at/",
			10,
			[],
			[{ "e": 930 }],
			[{ "v": 930 }],
			[{ "wait": 500 }, { "c": 930 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 930
			}],
			{}
		],
		[
			1,
			"auto_DE_daad.de_w45",
			0,
			"^https?://(www\\.)?daad\\.de/",
			10,
			[],
			[{ "e": 931 }],
			[{ "v": 931 }],
			[{ "wait": 500 }, { "c": 931 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 931
			}],
			{}
		],
		[
			1,
			"auto_DE_das-ist-drin.de_e12",
			0,
			"^https?://(www\\.)?das-ist-drin\\.de/",
			10,
			[],
			[{ "e": 932 }],
			[{ "v": 932 }],
			[{ "wait": 500 }, { "c": 932 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 932
			}],
			{}
		],
		[
			1,
			"auto_DE_de.accio.com_97o",
			0,
			"^https?://(www\\.)?de\\.accio\\.com/",
			10,
			[],
			[{ "e": 912 }],
			[{ "v": 912 }],
			[{ "wait": 500 }, { "c": 912 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 912
			}],
			{}
		],
		[
			1,
			"auto_DE_de.artprice.com_kfk",
			0,
			"^https?://(www\\.)?de\\.artprice\\.com/",
			10,
			[],
			[{ "e": 933 }],
			[{ "v": 933 }],
			[{ "wait": 500 }, { "c": 933 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 933
			}],
			{}
		],
		[
			1,
			"auto_DE_de.nothing.tech_0fr_+1",
			0,
			"^https?://(www\\.)?de\\.nothing\\.tech/|^https?://(www\\.)?nothing\\.tech/",
			10,
			[],
			[{ "e": 934 }],
			[{ "v": 934 }],
			[{ "wait": 500 }, { "c": 934 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 934
			}],
			{}
		],
		[
			1,
			"auto_DE_dekra.de_qrb",
			0,
			"^https?://(www\\.)?dekra\\.de/",
			10,
			[],
			[{ "e": 935 }],
			[{ "v": 935 }],
			[{ "wait": 500 }, { "c": 935 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 935
			}],
			{}
		],
		[
			1,
			"auto_DE_edelstahl-tuerklingel.de_375",
			0,
			"^https?://(www\\.)?edelstahl-tuerklingel\\.de/",
			10,
			[],
			[{ "e": 936 }],
			[{ "v": 936 }],
			[{ "wait": 500 }, { "c": 936 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 936
			}],
			{}
		],
		[
			1,
			"auto_DE_eezy.nrw_9aj",
			0,
			"^https?://(www\\.)?eezy\\.nrw/",
			10,
			[],
			[{ "e": 937 }],
			[{ "v": 937 }],
			[{ "wait": 500 }, { "c": 937 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 937
			}],
			{}
		],
		[
			1,
			"auto_DE_ernstings-family.de_xqf",
			0,
			"^https?://(www\\.)?ernstings-family\\.de/",
			10,
			[],
			[{ "e": 938 }],
			[{ "v": 938 }],
			[{ "wait": 500 }, { "c": 938 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 938
			}],
			{}
		],
		[
			1,
			"auto_DE_fcbinside.de_0d6",
			0,
			"^https?://(www\\.)?fcbinside\\.de/",
			10,
			[],
			[{ "e": 939 }],
			[{ "v": 939 }],
			[{ "wait": 500 }, { "c": 939 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 939
			}],
			{}
		],
		[
			1,
			"auto_DE_feierabend.de_kr4",
			0,
			"^https?://(www\\.)?feierabend\\.de/",
			10,
			[],
			[{ "e": 940 }],
			[{ "v": 940 }],
			[{ "wait": 500 }, { "c": 940 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 940
			}],
			{}
		],
		[
			1,
			"auto_DE_feser-graf.de_qz8",
			0,
			"^https?://(www\\.)?feser-graf\\.de/",
			10,
			[],
			[{ "e": 941 }],
			[{ "v": 941 }],
			[{ "wait": 500 }, { "c": 941 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 941
			}],
			{}
		],
		[
			1,
			"auto_DE_finanzpartner.de_13s",
			0,
			"^https?://(www\\.)?finanzpartner\\.de/",
			10,
			[],
			[{ "e": 942 }],
			[{ "v": 942 }],
			[{ "wait": 500 }, { "c": 942 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 942
			}],
			{}
		],
		[
			1,
			"auto_DE_gasometer.de_0xw",
			0,
			"^https?://(www\\.)?gasometer\\.de/",
			10,
			[],
			[{ "e": 943 }],
			[{ "v": 943 }],
			[{ "wait": 500 }, { "c": 943 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 943
			}],
			{}
		],
		[
			1,
			"auto_DE_hermoney.de_jsi",
			0,
			"^https?://(www\\.)?hermoney\\.de/",
			10,
			[],
			[{ "e": 944 }],
			[{ "v": 944 }],
			[{ "wait": 500 }, { "c": 944 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 944
			}],
			{}
		],
		[
			1,
			"auto_DE_hilfe.kleinanzeigen.de_44a_+1",
			0,
			"^https?://(www\\.)?hilfe\\.kleinanzeigen\\.de/|^https?://(www\\.)?themen\\.kleinanzeigen\\.de/",
			10,
			[],
			[{ "e": 945 }],
			[{ "v": 945 }],
			[{ "wait": 500 }, { "c": 945 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 945
			}],
			{}
		],
		[
			1,
			"auto_DE_howik.com_99g",
			0,
			"^https?://(www\\.)?howik\\.com/",
			10,
			[],
			[{ "e": 946 }],
			[{ "v": 946 }],
			[{ "wait": 500 }, { "c": 946 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 946
			}],
			{}
		],
		[
			1,
			"auto_DE_hu-berlin.de_sk6",
			0,
			"^https?://(www\\.)?hu-berlin\\.de/",
			10,
			[],
			[{ "e": 947 }],
			[{ "v": 947 }],
			[{ "wait": 500 }, { "c": 947 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 947
			}],
			{}
		],
		[
			1,
			"auto_DE_imd-berlin.de_6m1",
			0,
			"^https?://(www\\.)?imd-berlin\\.de/",
			10,
			[],
			[{ "e": 948 }],
			[{ "v": 948 }],
			[{ "wait": 500 }, { "c": 948 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 948
			}],
			{}
		],
		[
			1,
			"auto_DE_immobilien.sparkasse.de_zj7",
			0,
			"^https?://(www\\.)?immobilien\\.sparkasse\\.de/",
			10,
			[949],
			[{ "e": 950 }],
			[{ "v": 950 }],
			[{ "c": 951 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 950
			}],
			{}
		],
		[
			1,
			"auto_DE_impfen-info.de_am5_+1",
			0,
			"^https?://(www\\.)?impfen-info\\.de/|^https?://(www\\.)?infektionsschutz\\.de/",
			10,
			[],
			[{ "e": 952 }],
			[{ "v": 952 }],
			[{ "wait": 500 }, { "c": 952 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 952
			}],
			{}
		],
		[
			1,
			"auto_DE_jobvector.de_641",
			0,
			"^https?://(www\\.)?jobvector\\.de/",
			10,
			[],
			[{ "e": 953 }],
			[{ "v": 953 }],
			[{ "wait": 500 }, { "c": 953 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 953
			}],
			{}
		],
		[
			1,
			"auto_DE_kinsta.com_hc5",
			0,
			"^https?://(www\\.)?kinsta\\.com/",
			10,
			[],
			[{ "e": 954 }],
			[{ "v": 954 }],
			[{ "wait": 500 }, { "c": 954 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 954
			}],
			{}
		],
		[
			1,
			"auto_DE_kleineskraftwerk.de_fx7",
			0,
			"^https?://(www\\.)?kleineskraftwerk\\.de/",
			10,
			[],
			[{ "e": 955 }],
			[{ "v": 955 }],
			[{ "wait": 500 }, { "c": 955 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 955
			}],
			{}
		],
		[
			1,
			"auto_DE_kundenportal.m-net.de_y8l",
			0,
			"^https?://(www\\.)?kundenportal\\.m-net\\.de/",
			10,
			[],
			[{ "e": 904 }],
			[{ "v": 904 }],
			[{ "wait": 500 }, { "c": 904 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 904
			}],
			{}
		],
		[
			1,
			"auto_DE_kvhb.de_hhf",
			0,
			"^https?://(www\\.)?kvhb\\.de/",
			10,
			[],
			[{ "e": 956 }],
			[{ "v": 956 }],
			[{ "wait": 500 }, { "c": 956 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 956
			}],
			{}
		],
		[
			1,
			"auto_DE_la.spankbang.com_sva",
			0,
			"^https?://(www\\.)?la\\.spankbang\\.com/",
			10,
			[],
			[{ "e": 957 }],
			[{ "v": 957 }],
			[{ "wait": 500 }, { "c": 957 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 957
			}],
			{}
		],
		[
			1,
			"auto_DE_lbs.de_6zt",
			0,
			"^https?://(www\\.)?lbs\\.de/",
			10,
			[],
			[{ "e": 958 }],
			[{ "v": 958 }],
			[{ "wait": 500 }, { "c": 958 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 958
			}],
			{}
		],
		[
			1,
			"auto_DE_lenovo.com_xcv",
			0,
			"^https?://(www\\.)?lenovo\\.com/",
			10,
			[],
			[{ "e": 959 }],
			[{ "v": 959 }],
			[{ "wait": 500 }, { "c": 959 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 959
			}],
			{}
		],
		[
			1,
			"auto_DE_listando.de_c5i",
			0,
			"^https?://(www\\.)?listando\\.de/",
			10,
			[],
			[{ "e": 960 }],
			[{ "v": 960 }],
			[{ "wait": 500 }, { "c": 960 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 960
			}],
			{}
		],
		[
			1,
			"auto_DE_lite-magazin.de_e1s",
			0,
			"^https?://(www\\.)?lite-magazin\\.de/",
			10,
			[],
			[{ "e": 961 }],
			[{ "v": 961 }],
			[{ "wait": 500 }, { "c": 961 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 961
			}],
			{}
		],
		[
			1,
			"auto_DE_m.livejasmin.com_cvg",
			0,
			"^https?://(www\\.)?livejasmin\\.com/",
			10,
			[],
			[{ "e": 962 }],
			[{ "v": 962 }],
			[{ "wait": 500 }, { "c": 962 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 962
			}],
			{}
		],
		[
			1,
			"auto_DE_mhplus-krankenkasse.de_4xb",
			0,
			"^https?://(www\\.)?mhplus-krankenkasse\\.de/",
			10,
			[],
			[{ "e": 917 }],
			[{ "v": 917 }],
			[{ "wait": 500 }, { "c": 917 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 917
			}],
			{}
		],
		[
			1,
			"auto_DE_mitarbeiterservice.bayern.de_quh",
			0,
			"^https?://(www\\.)?mitarbeiterservice\\.bayern\\.de/",
			10,
			[],
			[{ "e": 963 }],
			[{ "v": 963 }],
			[{ "wait": 500 }, { "c": 963 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 963
			}],
			{}
		],
		[
			1,
			"auto_DE_mrmarvis.com_bo5",
			0,
			"^https?://(www\\.)?mrmarvis\\.com/",
			10,
			[],
			[{ "e": 964 }],
			[{ "v": 964 }],
			[{ "wait": 500 }, { "c": 964 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 964
			}],
			{}
		],
		[
			1,
			"auto_DE_nanu-nana.de_7my",
			0,
			"^https?://(www\\.)?nanu-nana\\.de/",
			10,
			[],
			[{ "e": 965 }],
			[{ "v": 965 }],
			[{ "wait": 500 }, { "c": 965 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 965
			}],
			{}
		],
		[
			1,
			"auto_DE_originalteile.mercedes-benz.de_tce",
			0,
			"^https?://(www\\.)?originalteile\\.mercedes-benz\\.de/",
			10,
			[],
			[{ "e": 966 }],
			[{ "v": 966 }],
			[{ "wait": 500 }, { "c": 966 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 966
			}],
			{}
		],
		[
			1,
			"auto_DE_parqet.com_6wm",
			0,
			"^https?://(www\\.)?parqet\\.com/",
			10,
			[],
			[{ "e": 967 }],
			[{ "v": 967 }],
			[{ "wait": 500 }, { "c": 967 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 967
			}],
			{}
		],
		[
			1,
			"auto_DE_pflanzenhof-online.de_au2",
			0,
			"^https?://(www\\.)?pflanzenhof-online\\.de/",
			10,
			[],
			[{ "e": 968 }],
			[{ "v": 968 }],
			[{ "wait": 500 }, { "c": 968 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 968
			}],
			{}
		],
		[
			1,
			"auto_DE_polizei.hessen.de_rsx",
			0,
			"^https?://(www\\.)?polizei\\.hessen\\.de/",
			10,
			[],
			[{ "e": 969 }],
			[{ "v": 969 }],
			[{ "wait": 500 }, { "c": 969 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 969
			}],
			{}
		],
		[
			1,
			"auto_DE_regierung.oberbayern.bayern.de_zx2_+2",
			0,
			"^https?://(www\\.)?regierung\\.oberbayern\\.bayern\\.de/|^https?://(www\\.)?statistik\\.bayern\\.de/|^https?://(www\\.)?stmb\\.bayern\\.de/",
			10,
			[],
			[{ "e": 970 }],
			[{ "v": 970 }],
			[{ "wait": 500 }, { "c": 970 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 970
			}],
			{}
		],
		[
			1,
			"auto_DE_roller.de_pjo",
			0,
			"^https?://(www\\.)?roller\\.de/",
			10,
			[],
			[{ "e": 971 }],
			[{ "v": 971 }],
			[{ "wait": 500 }, { "c": 971 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 971
			}],
			{}
		],
		[
			1,
			"auto_DE_rundfunkbeitrag.de_g4y",
			0,
			"^https?://(www\\.)?rundfunkbeitrag\\.de/",
			10,
			[],
			[{ "e": 972 }],
			[{ "v": 972 }],
			[{ "wait": 500 }, { "c": 972 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 972
			}],
			{}
		],
		[
			1,
			"auto_DE_schwabach.de_fjr",
			0,
			"^https?://(www\\.)?schwabach\\.de/",
			10,
			[],
			[{ "e": 973 }],
			[{ "v": 973 }],
			[{ "wait": 500 }, { "c": 973 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 973
			}],
			{}
		],
		[
			1,
			"auto_DE_schwaebisch-hall.de_0g1",
			0,
			"^https?://(www\\.)?schwaebisch-hall\\.de/",
			10,
			[],
			[{ "e": 974 }],
			[{ "v": 974 }],
			[{ "wait": 500 }, { "c": 974 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 974
			}],
			{}
		],
		[
			1,
			"auto_DE_sellercentral.amazon.de_xi0",
			0,
			"^https?://(www\\.)?sellercentral\\.amazon\\.de/",
			10,
			[],
			[{ "e": 975 }],
			[{ "v": 975 }],
			[{ "wait": 500 }, { "c": 975 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 975
			}],
			{}
		],
		[
			1,
			"auto_DE_sephora.de_exg",
			0,
			"^https?://(www\\.)?sephora\\.de/",
			10,
			[],
			[{ "e": 897 }],
			[{ "v": 897 }],
			[{ "wait": 500 }, { "c": 897 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 897
			}],
			{}
		],
		[
			1,
			"auto_DE_solarspeicher24.de_w5k",
			0,
			"^https?://(www\\.)?solarspeicher24\\.de/",
			10,
			[],
			[{ "e": 976 }],
			[{ "v": 976 }],
			[{ "wait": 500 }, { "c": 976 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 976
			}],
			{}
		],
		[
			1,
			"auto_DE_speedtest.vodafone.de_dha",
			0,
			"^https?://(www\\.)?speedtest\\.vodafone\\.de/",
			10,
			[],
			[{ "e": 977 }],
			[{ "v": 977 }],
			[{ "wait": 500 }, { "c": 977 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 977
			}],
			{}
		],
		[
			1,
			"auto_DE_steeltoyz.de_i51",
			0,
			"^https?://(www\\.)?steeltoyz\\.de/",
			10,
			[],
			[{ "e": 978 }],
			[{ "v": 978 }],
			[{ "wait": 500 }, { "c": 978 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 978
			}],
			{}
		],
		[
			1,
			"auto_DE_survival-kompass.de_kv6",
			0,
			"^https?://(www\\.)?survival-kompass\\.de/",
			10,
			[],
			[{ "e": 979 }],
			[{ "v": 979 }],
			[{ "wait": 500 }, { "c": 979 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 979
			}],
			{}
		],
		[
			1,
			"auto_DE_typografie.info_mnj",
			0,
			"^https?://(www\\.)?typografie\\.info/",
			10,
			[],
			[{ "e": 980 }],
			[{ "v": 980 }],
			[{ "wait": 500 }, { "c": 980 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 980
			}],
			{}
		],
		[
			1,
			"auto_DE_uni-hildesheim.de_7kj",
			0,
			"^https?://(www\\.)?uni-hildesheim\\.de/",
			10,
			[],
			[{ "e": 981 }],
			[{ "v": 981 }],
			[{ "wait": 500 }, { "c": 981 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 981
			}],
			{}
		],
		[
			1,
			"auto_DE_uni-mannheim.de_omi",
			0,
			"^https?://(www\\.)?uni-mannheim\\.de/",
			10,
			[],
			[{ "e": 982 }],
			[{ "v": 982 }],
			[{ "wait": 500 }, { "c": 982 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 982
			}],
			{}
		],
		[
			1,
			"auto_DE_variete.de_6cc",
			0,
			"^https?://(www\\.)?variete\\.de/",
			10,
			[],
			[{ "e": 983 }],
			[{ "v": 983 }],
			[{ "wait": 500 }, { "c": 983 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 983
			}],
			{}
		],
		[
			1,
			"auto_DE_wien.gv.at_mm2",
			0,
			"^https?://(www\\.)?wien\\.gv\\.at/",
			10,
			[],
			[{ "e": 984 }],
			[{ "v": 984 }],
			[{ "wait": 500 }, { "c": 984 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 984
			}],
			{}
		],
		[
			1,
			"auto_DE_wolt.com_jyq",
			0,
			"^https?://(www\\.)?wolt\\.com/",
			10,
			[],
			[{ "e": 985 }],
			[{ "v": 985 }],
			[{ "wait": 500 }, { "c": 985 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 985
			}],
			{}
		],
		[
			1,
			"auto_FR_3ds.com_pa7",
			0,
			"^https?://(www\\.)?3ds\\.com/",
			10,
			[],
			[{ "e": 986 }],
			[{ "v": 986 }],
			[{ "wait": 500 }, { "c": 986 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 986
			}],
			{}
		],
		[
			1,
			"auto_FR_aefinfo.fr_6r7",
			0,
			"^https?://(www\\.)?aefinfo\\.fr/",
			10,
			[],
			[{ "e": 987 }],
			[{ "v": 987 }],
			[{ "wait": 500 }, { "c": 987 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 987
			}],
			{}
		],
		[
			1,
			"auto_FR_alinea.com_d9k",
			0,
			"^https?://(www\\.)?alinea\\.com/",
			10,
			[],
			[{ "e": 988 }],
			[{ "v": 988 }],
			[{ "wait": 500 }, { "c": 988 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 988
			}],
			{}
		],
		[
			1,
			"auto_FR_alinea.com_gst",
			0,
			"^https?://(www\\.)?alinea\\.com/",
			10,
			[],
			[{ "e": 989 }],
			[{ "v": 989 }],
			[{ "wait": 500 }, { "c": 989 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 989
			}],
			{}
		],
		[
			1,
			"auto_FR_annuaire-inverse-france.com_4oi",
			0,
			"^https?://(www\\.)?annuaire-inverse-france\\.com/",
			10,
			[],
			[{ "e": 990 }],
			[{ "v": 990 }],
			[{ "wait": 500 }, { "c": 990 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 990
			}],
			{}
		],
		[
			1,
			"auto_FR_asp.gouv.fr_ytt",
			0,
			"^https?://(www\\.)?asp\\.gouv\\.fr/",
			10,
			[],
			[{ "e": 991 }],
			[{ "v": 991 }],
			[{ "wait": 500 }, { "c": 991 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 991
			}],
			{}
		],
		[
			1,
			"auto_FR_bd-adultes.com_nn2",
			0,
			"^https?://(www\\.)?bd-adultes\\.com/",
			10,
			[],
			[{ "e": 992 }],
			[{ "v": 992 }],
			[{ "wait": 500 }, { "c": 992 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 992
			}],
			{}
		],
		[
			1,
			"auto_FR_bobvoyeur.com_qm5",
			0,
			"^https?://(www\\.)?bobvoyeur\\.com/",
			10,
			[],
			[{ "e": 993 }],
			[{ "v": 993 }],
			[{ "wait": 500 }, { "c": 993 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 993
			}],
			{}
		],
		[
			1,
			"auto_FR_bpi.fr_l52",
			0,
			"^https?://(www\\.)?bpi\\.fr/",
			10,
			[],
			[{ "e": 842 }],
			[{ "v": 842 }],
			[{ "wait": 500 }, { "c": 842 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 842
			}],
			{}
		],
		[
			1,
			"auto_FR_chamrousse.com_i7u",
			0,
			"^https?://(www\\.)?chamrousse\\.com/",
			10,
			[],
			[{ "e": 994 }],
			[{ "v": 994 }],
			[{ "wait": 500 }, { "c": 994 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 994
			}],
			{}
		],
		[
			1,
			"auto_FR_charliehebdo.fr_smr",
			0,
			"^https?://(www\\.)?charliehebdo\\.fr/",
			10,
			[],
			[{ "e": 995 }],
			[{ "v": 995 }],
			[{ "wait": 500 }, { "c": 995 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 995
			}],
			{}
		],
		[
			1,
			"auto_FR_cite-sciences.fr_kcx",
			0,
			"^https?://(www\\.)?cite-sciences\\.fr/",
			10,
			[],
			[{ "e": 996 }],
			[{ "v": 996 }],
			[{ "wait": 500 }, { "c": 996 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 996
			}],
			{}
		],
		[
			1,
			"auto_FR_coe.int_cfo",
			0,
			"^https?://(www\\.)?coe\\.int/",
			10,
			[],
			[{ "e": 997 }],
			[{ "v": 997 }],
			[{ "wait": 500 }, { "c": 997 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 997
			}],
			{}
		],
		[
			1,
			"auto_FR_coutellerie-tourangelle.com_rcf",
			0,
			"^https?://(www\\.)?coutellerie-tourangelle\\.com/",
			10,
			[],
			[{ "e": 998 }],
			[{ "v": 998 }],
			[{ "wait": 500 }, { "c": 998 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 998
			}],
			{}
		],
		[
			1,
			"auto_FR_cre.fr_sd4",
			0,
			"^https?://(www\\.)?cre\\.fr/",
			10,
			[],
			[{ "e": 999 }],
			[{ "v": 999 }],
			[{ "wait": 500 }, { "c": 999 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 999
			}],
			{}
		],
		[
			1,
			"auto_FR_cybevasion.fr_jjp",
			0,
			"^https?://(www\\.)?cybevasion\\.fr/",
			10,
			[],
			[{ "e": 869 }],
			[{ "v": 869 }],
			[{ "wait": 500 }, { "c": 869 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 869
			}],
			{}
		],
		[
			1,
			"auto_FR_edumoov.com_sij",
			0,
			"^https?://(www\\.)?edumoov\\.com/",
			10,
			[],
			[{ "e": 1e3 }],
			[{ "v": 1e3 }],
			[{ "wait": 500 }, { "c": 1e3 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1e3
			}],
			{}
		],
		[
			1,
			"auto_FR_engie-homeservices.fr_lyo",
			0,
			"^https?://(www\\.)?engie-homeservices\\.fr/",
			10,
			[],
			[{ "e": 1001 }],
			[{ "v": 1001 }],
			[{ "wait": 500 }, { "c": 1001 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1001
			}],
			{}
		],
		[
			1,
			"auto_FR_es.xhamster.com_f69",
			0,
			"^https?://(www\\.)?es\\.xhamster\\.com/",
			10,
			[],
			[{ "e": 1002 }],
			[{ "v": 1002 }],
			[{ "wait": 500 }, { "c": 1002 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1002
			}],
			{}
		],
		[
			1,
			"auto_FR_euro-expos.com_tok",
			0,
			"^https?://(www\\.)?euro-expos\\.com/",
			10,
			[],
			[{ "e": 1003 }],
			[{ "v": 1003 }],
			[{ "wait": 500 }, { "c": 1003 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1003
			}],
			{}
		],
		[
			1,
			"auto_FR_fr.accio.com_zdn",
			0,
			"^https?://(www\\.)?fr\\.accio\\.com/",
			10,
			[],
			[{ "e": 912 }],
			[{ "v": 912 }],
			[{ "wait": 500 }, { "c": 912 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 912
			}],
			{}
		],
		[
			1,
			"auto_FR_fr.xgimi.com_fzb_+1",
			0,
			"^https?://(www\\.)?fr\\.xgimi\\.com/|^https?://(www\\.)?leminor\\.fr/",
			10,
			[],
			[{ "e": 821 }],
			[{ "v": 821 }],
			[{ "wait": 500 }, { "c": 821 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 821
			}],
			{}
		],
		[
			1,
			"auto_FR_glamuse.com_n32",
			0,
			"^https?://(www\\.)?glamuse\\.com/",
			10,
			[],
			[{ "e": 1004 }],
			[{ "v": 1004 }],
			[{ "wait": 500 }, { "c": 1004 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1004
			}],
			{}
		],
		[
			1,
			"auto_FR_gmf.fr_pxt",
			0,
			"^https?://(www\\.)?gmf\\.fr/",
			10,
			[],
			[{ "e": 1005 }],
			[{ "v": 1005 }],
			[{ "wait": 500 }, { "c": 1005 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1005
			}],
			{}
		],
		[
			1,
			"auto_FR_gov.br_n2f",
			0,
			"^https?://(www\\.)?gov\\.br/",
			10,
			[],
			[{ "e": 1006 }],
			[{ "v": 1006 }],
			[{ "wait": 500 }, { "c": 1006 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1006
			}],
			{}
		],
		[
			1,
			"auto_FR_greengo.voyage_fg3",
			0,
			"^https?://(www\\.)?greengo\\.voyage/",
			10,
			[],
			[{ "e": 1007 }],
			[{ "v": 1007 }],
			[{ "wait": 500 }, { "c": 1007 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1007
			}],
			{}
		],
		[
			1,
			"auto_FR_haproxy.com_arh",
			0,
			"^https?://(www\\.)?haproxy\\.com/",
			10,
			[],
			[{ "e": 1008 }],
			[{ "v": 1008 }],
			[{ "wait": 500 }, { "c": 1008 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1008
			}],
			{}
		],
		[
			1,
			"auto_FR_interencheres.com_c67",
			0,
			"^https?://(www\\.)?interencheres\\.com/",
			10,
			[],
			[{ "e": 1003 }],
			[{ "v": 1003 }],
			[{ "wait": 500 }, { "c": 1003 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1003
			}],
			{}
		],
		[
			1,
			"auto_FR_ita.xhamster.com_jhk",
			0,
			"^https?://(www\\.)?ita\\.xhamster\\.com/",
			10,
			[],
			[{ "e": 1002 }],
			[{ "v": 1002 }],
			[{ "wait": 500 }, { "c": 1002 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1002
			}],
			{}
		],
		[
			1,
			"auto_FR_kobo.com_ajz",
			0,
			"^https?://(www\\.)?kobo\\.com/",
			10,
			[],
			[{ "e": 1009 }],
			[{ "v": 1009 }],
			[{ "wait": 500 }, { "c": 1009 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1009
			}],
			{}
		],
		[
			1,
			"auto_FR_lalibrairie.com_0lt",
			0,
			"^https?://(www\\.)?lalibrairie\\.com/",
			10,
			[],
			[{ "e": 1010 }],
			[{ "v": 1010 }],
			[{ "wait": 500 }, { "c": 1010 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1010
			}],
			{}
		],
		[
			1,
			"auto_FR_leclercvoyages.com_2o4",
			0,
			"^https?://(www\\.)?leclercvoyages\\.com/",
			10,
			[],
			[{ "e": 1005 }],
			[{ "v": 1005 }],
			[{ "wait": 500 }, { "c": 1005 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1005
			}],
			{}
		],
		[
			1,
			"auto_FR_lesprosdelapetiteenfance.fr_bng",
			0,
			"^https?://(www\\.)?lesprosdelapetiteenfance\\.fr/",
			10,
			[],
			[{ "e": 873 }],
			[{ "v": 873 }],
			[{ "wait": 500 }, { "c": 873 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 873
			}],
			{}
		],
		[
			1,
			"auto_FR_ludum.fr_gl5",
			0,
			"^https?://(www\\.)?ludum\\.fr/",
			10,
			[],
			[{ "e": 1011 }],
			[{ "v": 1011 }],
			[{ "wait": 500 }, { "c": 1011 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1011
			}],
			{}
		],
		[
			1,
			"auto_FR_maboussoleaidants.fr_f8f",
			0,
			"^https?://(www\\.)?maboussoleaidants\\.fr/",
			10,
			[],
			[{ "e": 1012 }],
			[{ "v": 1012 }],
			[{ "wait": 500 }, { "c": 1012 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1012
			}],
			{}
		],
		[
			1,
			"auto_FR_magellan-bio.fr_5xr",
			0,
			"^https?://(www\\.)?magellan-bio\\.fr/",
			10,
			[],
			[{ "e": 1013 }],
			[{ "v": 1013 }],
			[{ "wait": 500 }, { "c": 1013 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1013
			}],
			{}
		],
		[
			1,
			"auto_FR_manuels.solutions_3gb",
			0,
			"^https?://(www\\.)?manuels\\.solutions/",
			10,
			[],
			[{ "e": 1014 }],
			[{ "v": 1014 }],
			[{ "wait": 500 }, { "c": 1014 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1014
			}],
			{}
		],
		[
			1,
			"auto_FR_maty.com_2v7",
			0,
			"^https?://(www\\.)?maty\\.com/",
			10,
			[],
			[{ "e": 904 }],
			[{ "v": 904 }],
			[{ "wait": 500 }, { "c": 904 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 904
			}],
			{}
		],
		[
			1,
			"auto_FR_mawaqit.net_0cw",
			0,
			"^https?://(www\\.)?mawaqit\\.net/",
			10,
			[],
			[{ "e": 1015 }],
			[{ "v": 1015 }],
			[{ "wait": 500 }, { "c": 1015 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1015
			}],
			{}
		],
		[
			1,
			"auto_FR_meformerenregion.fr_64b",
			0,
			"^https?://(www\\.)?meformerenregion\\.fr/",
			10,
			[],
			[{ "e": 1016 }],
			[{ "v": 1016 }],
			[{ "wait": 500 }, { "c": 1016 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1016
			}],
			{}
		],
		[
			1,
			"auto_FR_mesinfos.fr_gt2",
			0,
			"^https?://(www\\.)?mesinfos\\.fr/",
			10,
			[],
			[{ "e": 1017 }],
			[{ "v": 1017 }],
			[{ "wait": 500 }, { "c": 1017 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1017
			}],
			{}
		],
		[
			1,
			"auto_FR_naval-group.com_yzx",
			0,
			"^https?://(www\\.)?naval-group\\.com/",
			10,
			[],
			[{ "e": 873 }],
			[{ "v": 873 }],
			[{ "wait": 500 }, { "c": 873 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 873
			}],
			{}
		],
		[
			1,
			"auto_FR_norauto.fr_mbi",
			0,
			"^https?://(www\\.)?norauto\\.fr/",
			10,
			[],
			[{ "e": 904 }],
			[{ "v": 904 }],
			[{ "wait": 500 }, { "c": 904 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 904
			}],
			{}
		],
		[
			1,
			"auto_FR_nouslib.com_1tl",
			0,
			"^https?://(www\\.)?nouslib\\.com/",
			10,
			[],
			[{ "e": 1018 }],
			[{ "v": 1018 }],
			[{ "wait": 500 }, { "c": 1018 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1018
			}],
			{}
		],
		[
			1,
			"auto_FR_oceane.breizhgo.bzh_g7u",
			0,
			"^https?://(www\\.)?oceane\\.breizhgo\\.bzh/",
			10,
			[],
			[{ "e": 1019 }],
			[{ "v": 1019 }],
			[{ "wait": 500 }, { "c": 1019 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1019
			}],
			{}
		],
		[
			1,
			"auto_FR_pfg.fr_j2d_+1",
			0,
			"^https?://(www\\.)?pfg\\.fr/|^https?://(www\\.)?pointp\\.fr/",
			10,
			[],
			[{ "e": 1005 }],
			[{ "v": 1005 }],
			[{ "wait": 500 }, { "c": 1005 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1005
			}],
			{}
		],
		[
			1,
			"auto_FR_picwish.com_l0k",
			0,
			"^https?://(www\\.)?picwish\\.com/",
			10,
			[],
			[{ "e": 1020 }],
			[{ "v": 1020 }],
			[{ "wait": 500 }, { "c": 1020 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1020
			}],
			{}
		],
		[
			1,
			"auto_FR_platform.openai.com_nyz",
			0,
			"^https?://(www\\.)?platform\\.openai\\.com/",
			10,
			[],
			[{ "e": 1021 }],
			[{ "v": 1021 }],
			[{ "wait": 500 }, { "c": 1021 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1021
			}],
			{}
		],
		[
			1,
			"auto_FR_pointdevente.parionssport.fdj.fr_9uh",
			0,
			"^https?://(www\\.)?pointdevente\\.parionssport\\.fdj\\.fr/",
			10,
			[],
			[{ "e": 958 }],
			[{ "v": 958 }],
			[{ "wait": 500 }, { "c": 958 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 958
			}],
			{}
		],
		[
			1,
			"auto_FR_politis.fr_g33",
			0,
			"^https?://(www\\.)?politis\\.fr/",
			10,
			[],
			[{ "e": 1022 }],
			[{ "v": 1022 }],
			[{ "wait": 500 }, { "c": 1022 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1022
			}],
			{}
		],
		[
			1,
			"auto_FR_pretto.fr_kb5",
			0,
			"^https?://(www\\.)?pretto\\.fr/",
			10,
			[],
			[{ "e": 1023 }],
			[{ "v": 1023 }],
			[{ "wait": 500 }, { "c": 1023 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1023
			}],
			{}
		],
		[
			1,
			"auto_FR_privateaser.com_uco",
			0,
			"^https?://(www\\.)?privateaser\\.com/",
			10,
			[],
			[{ "e": 1024 }],
			[{ "v": 1024 }],
			[{ "wait": 500 }, { "c": 1024 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1024
			}],
			{}
		],
		[
			1,
			"auto_FR_pro.inserm.fr_omt",
			0,
			"^https?://(www\\.)?pro\\.inserm\\.fr/",
			10,
			[],
			[{ "e": 842 }],
			[{ "v": 842 }],
			[{ "wait": 500 }, { "c": 842 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 842
			}],
			{}
		],
		[
			1,
			"auto_FR_proantic.com_oyg",
			0,
			"^https?://(www\\.)?proantic\\.com/",
			10,
			[],
			[{ "e": 1025 }],
			[{ "v": 1025 }],
			[{ "wait": 500 }, { "c": 1025 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1025
			}],
			{}
		],
		[
			1,
			"auto_FR_revue-histoire.fr_jex",
			0,
			"^https?://(www\\.)?revue-histoire\\.fr/",
			10,
			[],
			[{ "e": 1026 }],
			[{ "v": 1026 }],
			[{ "wait": 500 }, { "c": 1026 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1026
			}],
			{}
		],
		[
			1,
			"auto_FR_rhinoshield.fr_k5l",
			0,
			"^https?://(www\\.)?rhinoshield\\.fr/",
			10,
			[],
			[{ "e": 821 }],
			[{ "v": 821 }],
			[{ "wait": 500 }, { "c": 821 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 821
			}],
			{}
		],
		[
			1,
			"auto_FR_sephora.fr_k3l",
			0,
			"^https?://(www\\.)?sephora\\.fr/",
			10,
			[],
			[{ "e": 897 }],
			[{ "v": 897 }],
			[{ "wait": 500 }, { "c": 897 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 897
			}],
			{}
		],
		[
			1,
			"auto_FR_xhamster.desi_pv1_+1",
			0,
			"^https?://(www\\.)?xhamster\\.desi/|^https?://(www\\.)?xhamster3\\.com/",
			10,
			[],
			[{ "e": 1002 }],
			[{ "v": 1002 }],
			[{ "wait": 500 }, { "c": 1002 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1002
			}],
			{}
		],
		[
			1,
			"auto_GB_3djake.uk_0",
			0,
			"^https?://(www\\.)?3djake\\.uk/",
			10,
			[],
			[{ "e": 1027 }],
			[{ "v": 1027 }],
			[{ "c": 1027 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_actionfraud.org.uk_92k",
			0,
			"^https?://(www\\.)?actionfraud\\.org\\.uk/",
			10,
			[],
			[{ "e": 1028 }],
			[{ "v": 1028 }],
			[{ "wait": 500 }, { "c": 1028 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1028
			}],
			{}
		],
		[
			1,
			"auto_GB_ancestry.com_0",
			0,
			"^https?://(www\\.)?ancestry\\.com/",
			10,
			[],
			[{ "e": 1029 }],
			[{ "v": 1029 }],
			[{ "wait": 500 }, { "c": 1029 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1029
			}],
			{}
		],
		[
			1,
			"auto_GB_arte.tv_0",
			0,
			"^https?://(www\\.)?arte\\.tv/",
			10,
			[],
			[{ "e": 1030 }],
			[{ "v": 1030 }],
			[{ "c": 1030 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_bensnaturalhealth.co.uk_0",
			0,
			"^https?://(www\\.)?bensnaturalhealth\\.co\\.uk/",
			10,
			[],
			[{ "e": 1031 }],
			[{ "v": 1031 }],
			[{ "c": 1031 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_bike24.com_0",
			0,
			"^https?://(www\\.)?bike24\\.com/",
			10,
			[],
			[{ "e": 1032 }],
			[{ "v": 1032 }],
			[{ "c": 1032 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_brazzers.com_0",
			0,
			"^https?://(www\\.)?brazzers\\.com/",
			10,
			[],
			[{ "e": 1033 }],
			[{ "v": 1033 }],
			[{ "c": 1033 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_bricksandlogic.co.uk_o5o",
			0,
			"^https?://(www\\.)?bricksandlogic\\.co\\.uk/",
			10,
			[],
			[{ "e": 1034 }],
			[{ "v": 1034 }],
			[{ "wait": 500 }, { "c": 1034 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1034
			}],
			{}
		],
		[
			1,
			"auto_GB_brightondome.org_iz9",
			0,
			"^https?://(www\\.)?brightondome\\.org/",
			10,
			[],
			[{ "e": 1035 }],
			[{ "v": 1035 }],
			[{ "wait": 500 }, { "c": 1035 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1035
			}],
			{}
		],
		[
			1,
			"auto_GB_businessclass.com_0",
			0,
			"^https?://(www\\.)?businessclass\\.com/",
			10,
			[],
			[{ "e": 1036 }],
			[{ "v": 1036 }],
			[{ "c": 1036 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_capcut.com_0",
			0,
			"^https?://(www\\.)?capcut\\.com/",
			10,
			[],
			[{ "e": 1037 }],
			[{ "v": 1037 }],
			[{ "c": 1037 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_cardmarket.com_oxh",
			0,
			"^https?://(www\\.)?cardmarket\\.com/",
			10,
			[],
			[{ "e": 1038 }],
			[{ "v": 1038 }],
			[{ "wait": 500 }, { "c": 1038 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1038
			}],
			{}
		],
		[
			1,
			"auto_GB_catawiki.com_0",
			0,
			"^https?://(www\\.)?catawiki\\.com/",
			10,
			[],
			[{ "e": 1039 }],
			[{ "v": 1039 }],
			[{ "c": 1039 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_charlesclinkard.co.uk_0",
			0,
			"^https?://(www\\.)?charlesclinkard\\.co\\.uk/",
			10,
			[],
			[{ "e": 1040 }],
			[{ "v": 1040 }],
			[{ "wait": 500 }, { "c": 1040 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1040
			}],
			{}
		],
		[
			1,
			"auto_GB_chilternseeds.co.uk_0",
			0,
			"^https?://(www\\.)?chilternseeds\\.co\\.uk/",
			10,
			[],
			[{ "e": 1041 }],
			[{ "v": 1041 }],
			[{ "wait": 500 }, { "c": 1041 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1041
			}],
			{}
		],
		[
			1,
			"auto_GB_chrono24.com_0",
			0,
			"^https?://(www\\.)?chrono24\\.com/",
			10,
			[],
			[{ "e": 1042 }],
			[{ "v": 1042 }],
			[{ "c": 1042 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_cpfc.co.uk_0",
			0,
			"^https?://(www\\.)?cpfc\\.co\\.uk/",
			10,
			[],
			[{ "e": 1043 }],
			[{ "v": 1043 }],
			[{ "c": 1043 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_deezer.com_0",
			0,
			"^https?://(www\\.)?deezer\\.com/",
			10,
			[],
			[{ "e": 1044 }],
			[{ "v": 1044 }],
			[{ "c": 1044 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_edinburghcastle.scot_h2e",
			0,
			"^https?://(www\\.)?edinburghcastle\\.scot/",
			10,
			[],
			[{ "e": 1045 }],
			[{ "v": 1045 }],
			[{ "wait": 500 }, { "c": 1045 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1045
			}],
			{}
		],
		[
			1,
			"auto_GB_europarl.europa.eu_0",
			0,
			"^https?://(www\\.)?europarl\\.europa\\.eu/",
			10,
			[],
			[{ "e": 1046 }],
			[{ "v": 1046 }],
			[{ "c": 1046 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_everysaving.co.uk_38t",
			0,
			"^https?://(www\\.)?everysaving\\.co\\.uk/",
			10,
			[],
			[{ "e": 1047 }],
			[{ "v": 1047 }],
			[{ "c": 1047 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_ewrc-results.com_y5f",
			0,
			"^https?://(www\\.)?ewrc-results\\.com/",
			10,
			[],
			[{ "e": 1048 }],
			[{ "v": 1048 }],
			[{ "wait": 500 }, { "c": 1048 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1048
			}],
			{}
		],
		[
			1,
			"auto_GB_f6s.com_221",
			0,
			"^https?://(www\\.)?f6s\\.com/",
			10,
			[],
			[{ "e": 1049 }],
			[{ "v": 1049 }],
			[{ "wait": 500 }, { "c": 1049 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1049
			}],
			{}
		],
		[
			1,
			"auto_GB_faphouse.com_0",
			0,
			"^https?://(www\\.)?faphouse\\.com/",
			10,
			[],
			[{ "e": 1050 }],
			[{ "v": 1050 }],
			[{ "c": 1050 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_farmergracy.co.uk_dl3",
			0,
			"^https?://(www\\.)?farmergracy\\.co\\.uk/",
			10,
			[],
			[{ "e": 1051 }],
			[{ "v": 1051 }],
			[{ "wait": 500 }, { "c": 1051 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1051
			}],
			{}
		],
		[
			1,
			"auto_GB_fca.org.uk_9p9",
			0,
			"^https?://(www\\.)?fca\\.org\\.uk/",
			10,
			[],
			[{ "e": 1052 }],
			[{ "v": 1052 }],
			[{ "wait": 500 }, { "c": 1052 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1052
			}],
			{}
		],
		[
			1,
			"auto_GB_forum.affinity.serif.com_0",
			0,
			"^https?://(www\\.)?forum\\.affinity\\.serif\\.com/",
			10,
			[],
			[{ "e": 1053 }],
			[{ "v": 1053 }],
			[{ "c": 1053 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_garden4less.co.uk_0",
			0,
			"^https?://(www\\.)?garden4less\\.co\\.uk/",
			10,
			[],
			[{ "e": 1054 }],
			[{ "v": 1054 }],
			[{ "c": 1054 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_glassesdirect.co.uk_bt9",
			0,
			"^https?://(www\\.)?glassesdirect\\.co\\.uk/",
			10,
			[],
			[{ "e": 1055 }],
			[{ "v": 1055 }],
			[{ "wait": 500 }, { "c": 1055 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1055
			}],
			{}
		],
		[
			1,
			"auto_GB_handbook.fca.org.uk_0",
			0,
			"^https?://(www\\.)?handbook\\.fca\\.org\\.uk/",
			10,
			[],
			[{ "e": 1056 }],
			[{ "v": 1056 }],
			[{ "wait": 500 }, { "c": 1056 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1056
			}],
			{}
		],
		[
			1,
			"auto_GB_historicenvironment.scot_0",
			0,
			"^https?://(www\\.)?historicenvironment\\.scot/",
			10,
			[],
			[{ "e": 1057 }],
			[{ "v": 1057 }],
			[{ "c": 1057 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_ionos.co.uk_c0a",
			0,
			"^https?://(www\\.)?ionos\\.co\\.uk/",
			10,
			[],
			[{ "e": 1058 }],
			[{ "v": 1058 }],
			[{ "wait": 500 }, { "c": 1058 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1058
			}],
			{}
		],
		[
			1,
			"auto_GB_kick.com_0",
			0,
			"^https?://(www\\.)?kick\\.com/",
			10,
			[],
			[{ "e": 1059 }],
			[{ "v": 1059 }],
			[{ "c": 1059 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_kinopoisk.ru_0",
			0,
			"^https?://(www\\.)?kinopoisk\\.ru/",
			10,
			[],
			[{ "e": 1060 }],
			[{ "v": 1060 }],
			[{ "wait": 500 }, { "c": 1060 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1060
			}],
			{}
		],
		[
			1,
			"auto_GB_kirklees.gov.uk_0",
			0,
			"^https?://(www\\.)?kirklees\\.gov\\.uk/",
			10,
			[],
			[{ "e": 1061 }],
			[{ "v": 1061 }],
			[{ "c": 1061 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_lancaster.ac.uk_0",
			0,
			"^https?://(www\\.)?lancaster\\.ac\\.uk/",
			10,
			[],
			[{ "e": 1062 }],
			[{ "v": 1062 }],
			[{ "c": 1062 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_lustery.com_0",
			0,
			"^https?://(www\\.)?lustery\\.com/",
			10,
			[],
			[{ "e": 1063 }],
			[{ "v": 1063 }],
			[{ "c": 1063 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_m.yandex.com_0_+2",
			0,
			"^https?://(www\\.)?m\\.yandex\\.com/|^https?://(www\\.)?online\\.yandex\\.com/|^https?://(www\\.)?xmlsearch\\.yandex\\.ru/",
			10,
			[],
			[{ "e": 1064 }],
			[{ "v": 1064 }],
			[{
				"text": "Allow essential cookies",
				"c": 1064
			}],
			[],
			{}
		],
		[
			1,
			"auto_GB_mypharmacy.co.uk_0",
			0,
			"^https?://(www\\.)?mypharmacy\\.co\\.uk/",
			10,
			[],
			[{ "e": 1065 }],
			[{ "v": 1065 }],
			[{
				"text": "DENY ALL",
				"c": 1065
			}],
			[],
			{}
		],
		[
			1,
			"auto_GB_onestream.co.uk_dpx",
			0,
			"^https?://(www\\.)?onestream\\.co\\.uk/",
			10,
			[],
			[{ "e": 1066 }],
			[{ "v": 1066 }],
			[{ "wait": 500 }, { "c": 1066 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1066
			}],
			{}
		],
		[
			1,
			"auto_GB_outfox.energy_6ux",
			0,
			"^https?://(www\\.)?outfox\\.energy/",
			10,
			[],
			[{ "e": 1067 }],
			[{ "v": 1067 }],
			[{ "wait": 500 }, { "c": 1067 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1067
			}],
			{}
		],
		[
			1,
			"auto_GB_parliamentlive.tv_r3v",
			0,
			"^https?://(www\\.)?parliamentlive\\.tv/",
			10,
			[],
			[{ "e": 1068 }],
			[{ "v": 1068 }],
			[{ "c": 1068 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_partscentre.co.uk_s70",
			0,
			"^https?://(www\\.)?partscentre\\.co\\.uk/",
			10,
			[],
			[{ "e": 1069 }],
			[{ "v": 1069 }],
			[{ "wait": 500 }, { "c": 1069 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1069
			}],
			{}
		],
		[
			1,
			"auto_GB_plumbingworld.co.uk_vmi",
			0,
			"^https?://(www\\.)?plumbingworld\\.co\\.uk/",
			10,
			[],
			[{ "e": 1070 }],
			[{ "v": 1070 }],
			[{ "wait": 500 }, { "c": 1070 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1070
			}],
			{}
		],
		[
			1,
			"auto_GB_reading.gov.uk_0",
			0,
			"^https?://(www\\.)?reading\\.gov\\.uk/",
			10,
			[],
			[{ "e": 1071 }],
			[{ "v": 1071 }],
			[{ "wait": 500 }, { "c": 1071 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1071
			}],
			{}
		],
		[
			1,
			"auto_GB_shopify.com_0",
			0,
			"^https?://(www\\.)?shopify\\.com/",
			10,
			[],
			[{ "e": 1072 }],
			[{ "v": 1072 }],
			[{ "c": 1072 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_sso.passport.yandex.ru_0_+4",
			0,
			"^https?://(www\\.)?sso\\.passport\\.yandex\\.ru/|^https?://(www\\.)?translate\\.yandex\\.com/|^https?://(www\\.)?ya\\.ru/|^https?://(www\\.)?yandex\\.com\\.tr/|^https?://(www\\.)?yandex\\.com/",
			10,
			[],
			[{ "e": 1060 }],
			[{ "v": 1060 }],
			[{ "c": 1060 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_stoneacre.co.uk_73c",
			0,
			"^https?://(www\\.)?stoneacre\\.co\\.uk/",
			10,
			[],
			[{ "e": 1073 }],
			[{ "v": 1073 }],
			[{ "wait": 500 }, { "c": 1073 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1073
			}],
			{}
		],
		[
			1,
			"auto_GB_supremecourt.uk_0",
			0,
			"^https?://(www\\.)?supremecourt\\.uk/",
			10,
			[],
			[{ "e": 1074 }],
			[{ "v": 1074 }],
			[{ "c": 1074 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1074
			}],
			{}
		],
		[
			1,
			"auto_GB_thebatteryshop.co.uk_0",
			0,
			"^https?://(www\\.)?thebatteryshop\\.co\\.uk/",
			10,
			[],
			[{ "e": 1075 }],
			[{ "v": 1075 }],
			[{
				"text": "Reject All",
				"c": 1075
			}],
			[],
			{}
		],
		[
			1,
			"auto_GB_thebushcraftstore.co.uk_0_+1",
			0,
			"^https?://(www\\.)?thebushcraftstore\\.co\\.uk/|^https?://(www\\.)?thewoolfactory\\.co\\.uk/",
			10,
			[],
			[{ "e": 1076 }],
			[{ "v": 1076 }],
			[{ "wait": 500 }, { "c": 1076 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1076
			}],
			{}
		],
		[
			1,
			"auto_GB_trove.scot_xtg",
			0,
			"^https?://(www\\.)?trove\\.scot/",
			10,
			[],
			[{ "e": 1077 }],
			[{ "v": 1077 }],
			[{ "wait": 500 }, { "c": 1077 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1077
			}],
			{}
		],
		[
			1,
			"auto_GB_truecaller.com_0",
			0,
			"^https?://(www\\.)?truecaller\\.com/",
			10,
			[],
			[{ "e": 1078 }],
			[{ "v": 1078 }],
			[{ "c": 1078 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_tube8.com_0_+1",
			0,
			"^https?://(www\\.)?tube8\\.com/|^https?://(www\\.)?youporn\\.com/",
			10,
			[],
			[{ "e": 1079 }],
			[{ "v": 1079 }],
			[{ "c": 1079 }],
			[],
			{}
		],
		[
			1,
			"auto_GB_vetuk.co.uk_0",
			0,
			"^https?://(www\\.)?vetuk\\.co\\.uk/",
			10,
			[],
			[{ "e": 1080 }],
			[{ "v": 1080 }],
			[{ "wait": 500 }, { "c": 1080 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1080
			}],
			{}
		],
		[
			1,
			"auto_GB_virgin.com_0",
			0,
			"^https?://(www\\.)?virgin\\.com/",
			10,
			[],
			[{ "e": 1081 }],
			[{ "v": 1081 }],
			[{ "wait": 500 }, { "c": 1081 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1081
			}],
			{}
		],
		[
			1,
			"auto_GB_vrhump.com_pb5",
			0,
			"^https?://(www\\.)?vrhump\\.com/",
			10,
			[],
			[{ "e": 1082 }],
			[{ "v": 1082 }],
			[{ "wait": 500 }, { "c": 1082 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1082
			}],
			{}
		],
		[
			1,
			"auto_GB_weldricks.co.uk_0",
			0,
			"^https?://(www\\.)?weldricks\\.co\\.uk/",
			10,
			[],
			[{ "e": 1083 }],
			[{ "v": 1083 }],
			[{ "wait": 500 }, { "c": 1083 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1083
			}],
			{}
		],
		[
			1,
			"auto_NL_3cx.com_0mf",
			0,
			"^https?://(www\\.)?3cx\\.com/",
			10,
			[],
			[{ "e": 1084 }],
			[{ "v": 1084 }],
			[{ "wait": 500 }, { "c": 1084 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1084
			}],
			{}
		],
		[
			1,
			"auto_NL_aegon.nl_y2y",
			0,
			"^https?://(www\\.)?aegon\\.nl/",
			10,
			[],
			[{ "e": 1085 }],
			[{ "v": 1085 }],
			[{ "wait": 500 }, { "c": 1085 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1085
			}],
			{}
		],
		[
			1,
			"auto_NL_app.chatgirl.nl_uqi",
			0,
			"^https?://(www\\.)?app\\.chatgirl\\.nl/",
			10,
			[],
			[{ "e": 1086 }],
			[{ "v": 1086 }],
			[{ "wait": 500 }, { "c": 1086 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1086
			}],
			{}
		],
		[
			1,
			"auto_NL_asnbank.nl_e28_+1",
			0,
			"^https?://(www\\.)?asnbank\\.nl/|^https?://(www\\.)?blgwonen\\.nl/",
			10,
			[],
			[{ "e": 1087 }],
			[{ "v": 1087 }],
			[{ "wait": 500 }, { "c": 1087 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1087
			}],
			{}
		],
		[
			1,
			"auto_NL_averoachmea.nl_fxj_+1",
			0,
			"^https?://(www\\.)?averoachmea\\.nl/|^https?://(www\\.)?centraalbeheer\\.nl/",
			10,
			[],
			[{ "e": 1088 }],
			[{ "v": 1088 }],
			[{ "wait": 500 }, { "c": 1088 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1088
			}],
			{}
		],
		[
			1,
			"auto_NL_beekman.nl_f9e",
			0,
			"^https?://(www\\.)?beekman\\.nl/",
			10,
			[],
			[{ "e": 1089 }],
			[{ "v": 1089 }],
			[{ "wait": 500 }, { "c": 1089 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1089
			}],
			{}
		],
		[
			1,
			"auto_NL_berivita.com_q3z",
			0,
			"^https?://(www\\.)?berivita\\.com/",
			10,
			[],
			[{ "e": 1090 }],
			[{ "v": 1090 }],
			[{ "wait": 500 }, { "c": 1090 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1090
			}],
			{}
		],
		[
			1,
			"auto_NL_bibliotheekaanzet.nl_mh6",
			0,
			"^https?://(www\\.)?bibliotheekaanzet\\.nl/",
			10,
			[],
			[{ "e": 1091 }],
			[{ "v": 1091 }],
			[{ "wait": 500 }, { "c": 1091 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1091
			}],
			{}
		],
		[
			1,
			"auto_NL_bk.nl_tx4",
			0,
			"^https?://(www\\.)?bk\\.nl/",
			10,
			[],
			[{ "e": 821 }],
			[{ "v": 821 }],
			[{ "wait": 500 }, { "c": 821 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 821
			}],
			{}
		],
		[
			1,
			"auto_NL_bonprix.nl_ovi",
			0,
			"^https?://(www\\.)?bonprix\\.nl/",
			10,
			[],
			[{ "e": 1092 }],
			[{ "v": 1092 }],
			[{ "wait": 500 }, { "c": 1092 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1092
			}],
			{}
		],
		[
			1,
			"auto_NL_braumarkt.com_wfi",
			0,
			"^https?://(www\\.)?braumarkt\\.com/",
			10,
			[],
			[{ "e": 966 }],
			[{ "v": 966 }],
			[{ "wait": 500 }, { "c": 966 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 966
			}],
			{}
		],
		[
			1,
			"auto_NL_brillen24.nl_08c",
			0,
			"^https?://(www\\.)?brillen24\\.nl/",
			10,
			[],
			[{ "e": 1093 }],
			[{ "v": 1093 }],
			[{ "wait": 500 }, { "c": 1093 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1093
			}],
			{}
		],
		[
			1,
			"auto_NL_chasse.nl_gs1",
			0,
			"^https?://(www\\.)?chasse\\.nl/",
			10,
			[],
			[{ "e": 1094 }],
			[{ "v": 1094 }],
			[{ "wait": 500 }, { "c": 1094 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1094
			}],
			{}
		],
		[
			1,
			"auto_NL_cheaptickets.nl_cwt",
			0,
			"^https?://(www\\.)?cheaptickets\\.nl/",
			10,
			[],
			[{ "e": 1095 }],
			[{ "v": 1095 }],
			[{ "wait": 500 }, { "c": 1095 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1095
			}],
			{}
		],
		[
			1,
			"auto_NL_chillplanet.nl_xcp",
			0,
			"^https?://(www\\.)?chillplanet\\.nl/",
			10,
			[],
			[{ "e": 1096 }],
			[{ "v": 1096 }],
			[{ "wait": 500 }, { "c": 1096 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1096
			}],
			{}
		],
		[
			1,
			"auto_NL_chrono24.nl_dco",
			0,
			"^https?://(www\\.)?chrono24\\.nl/",
			10,
			[],
			[{ "e": 870 }],
			[{ "v": 870 }],
			[{ "wait": 500 }, { "c": 870 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 870
			}],
			{}
		],
		[
			1,
			"auto_NL_clinicaldiagnostics.nl_pmu",
			0,
			"^https?://(www\\.)?clinicaldiagnostics\\.nl/",
			10,
			[],
			[{ "e": 1097 }],
			[{ "v": 1097 }],
			[{ "wait": 500 }, { "c": 1097 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1097
			}],
			{}
		],
		[
			1,
			"auto_NL_consumentenbond.nl_53g",
			0,
			"^https?://(www\\.)?consumentenbond\\.nl/",
			10,
			[],
			[{ "e": 1098 }],
			[{ "v": 1098 }],
			[{ "wait": 500 }, { "c": 1098 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1098
			}],
			{}
		],
		[
			1,
			"auto_NL_denboschregion.nl_4x6",
			0,
			"^https?://(www\\.)?denboschregion\\.nl/",
			10,
			[],
			[{ "e": 1099 }],
			[{ "v": 1099 }],
			[{ "wait": 500 }, { "c": 1099 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1099
			}],
			{}
		],
		[
			1,
			"auto_NL_denieuwebibliotheek.nl_c1z",
			0,
			"^https?://(www\\.)?denieuwebibliotheek\\.nl/",
			10,
			[],
			[{ "e": 1100 }],
			[{ "v": 1100 }],
			[{ "wait": 500 }, { "c": 1100 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1100
			}],
			{}
		],
		[
			1,
			"auto_NL_discountoffice.nl_2fb",
			0,
			"^https?://(www\\.)?discountoffice\\.nl/",
			10,
			[],
			[{ "e": 1101 }],
			[{ "v": 1101 }],
			[{ "wait": 500 }, { "c": 1101 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1101
			}],
			{}
		],
		[
			1,
			"auto_NL_ditjesendatjes.nl_0sa",
			0,
			"^https?://(www\\.)?ditjesendatjes\\.nl/",
			10,
			[],
			[{ "e": 1102 }],
			[{ "v": 1102 }],
			[{ "wait": 500 }, { "c": 1102 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1102
			}],
			{}
		],
		[
			1,
			"auto_NL_duitslandinstituut.nl_d4q",
			0,
			"^https?://(www\\.)?duitslandinstituut\\.nl/",
			10,
			[],
			[{ "e": 1103 }],
			[{ "v": 1103 }],
			[{ "wait": 500 }, { "c": 1103 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1103
			}],
			{}
		],
		[
			1,
			"auto_NL_effenaar.nl_nia",
			0,
			"^https?://(www\\.)?effenaar\\.nl/",
			10,
			[],
			[{ "e": 1104 }],
			[{ "v": 1104 }],
			[{ "wait": 500 }, { "c": 1104 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1104
			}],
			{}
		],
		[
			1,
			"auto_NL_eurojackpot.nederlandseloterij.nl_7hr",
			0,
			"^https?://(www\\.)?eurojackpot\\.nederlandseloterij\\.nl/",
			10,
			[],
			[{ "e": 1105 }],
			[{ "v": 1105 }],
			[{ "wait": 500 }, { "c": 1105 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1105
			}],
			{}
		],
		[
			1,
			"auto_NL_fietsonderdelenoutlet.nl_x8u",
			0,
			"^https?://(www\\.)?fietsonderdelenoutlet\\.nl/",
			10,
			[],
			[{ "e": 1106 }],
			[{ "v": 1106 }],
			[{ "wait": 500 }, { "c": 1106 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1106
			}],
			{}
		],
		[
			1,
			"auto_NL_followthebeat.nl_mx5",
			0,
			"^https?://(www\\.)?followthebeat\\.nl/",
			10,
			[],
			[{ "e": 1107 }],
			[{ "v": 1107 }],
			[{ "wait": 500 }, { "c": 1107 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1107
			}],
			{}
		],
		[
			1,
			"auto_NL_frankenergie.nl_9xh",
			0,
			"^https?://(www\\.)?frankenergie\\.nl/",
			10,
			[],
			[{ "e": 1108 }],
			[{ "v": 1108 }],
			[{ "wait": 500 }, { "c": 1108 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1108
			}],
			{}
		],
		[
			1,
			"auto_NL_gezondheidenwetenschap.be_zgi",
			0,
			"^https?://(www\\.)?gezondheidenwetenschap\\.be/",
			10,
			[],
			[{ "e": 1109 }],
			[{ "v": 1109 }],
			[{ "wait": 500 }, { "c": 1109 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1109
			}],
			{}
		],
		[
			1,
			"auto_NL_info.mumc.nl_8s5",
			0,
			"^https?://(www\\.)?info\\.mumc\\.nl/",
			10,
			[],
			[{ "e": 1110 }],
			[{ "v": 1110 }],
			[{ "wait": 500 }, { "c": 1110 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1110
			}],
			{}
		],
		[
			1,
			"auto_NL_inshared.nl_p70",
			0,
			"^https?://(www\\.)?inshared\\.nl/",
			10,
			[],
			[{ "e": 1111 }],
			[{ "v": 1111 }],
			[{ "wait": 500 }, { "c": 1111 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1111
			}],
			{}
		],
		[
			1,
			"auto_NL_isvw.nl_7x0",
			0,
			"^https?://(www\\.)?isvw\\.nl/",
			10,
			[],
			[{ "e": 1112 }],
			[{ "v": 1112 }],
			[{ "wait": 500 }, { "c": 1112 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1112
			}],
			{}
		],
		[
			1,
			"auto_NL_kaartje2go.nl_ecw",
			0,
			"^https?://(www\\.)?kaartje2go\\.nl/",
			10,
			[],
			[{ "e": 1113 }],
			[{ "v": 1113 }],
			[{ "wait": 500 }, { "c": 1113 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1113
			}],
			{}
		],
		[
			1,
			"auto_NL_kathmandu.nl_7j4",
			0,
			"^https?://(www\\.)?kathmandu\\.nl/",
			10,
			[],
			[{ "e": 1114 }],
			[{ "v": 1114 }],
			[{ "wait": 500 }, { "c": 1114 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1114
			}],
			{}
		],
		[
			1,
			"auto_NL_kvk.nl_0wn",
			0,
			"^https?://(www\\.)?kvk\\.nl/",
			10,
			[],
			[{ "e": 1115 }],
			[{ "v": 1115 }],
			[{ "wait": 500 }, { "c": 1115 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1115
			}],
			{}
		],
		[
			1,
			"auto_NL_labplusarts.nl_feg",
			0,
			"^https?://(www\\.)?labplusarts\\.nl/",
			10,
			[],
			[{ "e": 1116 }],
			[{ "v": 1116 }],
			[{ "wait": 500 }, { "c": 1116 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1116
			}],
			{}
		],
		[
			1,
			"auto_NL_lakenhal.nl_3go",
			0,
			"^https?://(www\\.)?lakenhal\\.nl/",
			10,
			[],
			[{ "e": 1117 }],
			[{ "v": 1117 }],
			[{ "wait": 500 }, { "c": 1117 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1117
			}],
			{}
		],
		[
			1,
			"auto_NL_lotto.nederlandseloterij.nl_7c8_+1",
			0,
			"^https?://(www\\.)?lotto\\.nederlandseloterij\\.nl/|^https?://(www\\.)?staatsloterij\\.nederlandseloterij\\.nl/",
			10,
			[],
			[{ "e": 1118 }],
			[{ "v": 1118 }],
			[{ "wait": 500 }, { "c": 1118 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1118
			}],
			{}
		],
		[
			1,
			"auto_NL_magazines-motivatie.nl_6o1",
			0,
			"^https?://(www\\.)?magazines-motivatie\\.nl/",
			10,
			[],
			[{ "e": 1119 }],
			[{ "v": 1119 }],
			[{ "wait": 500 }, { "c": 1119 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1119
			}],
			{}
		],
		[
			1,
			"auto_NL_makro.nl_ror",
			0,
			"^https?://(www\\.)?makro\\.nl/",
			10,
			[],
			[{ "e": 1120 }],
			[{ "v": 1120 }],
			[{ "wait": 500 }, { "c": 1120 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1120
			}],
			{}
		],
		[
			1,
			"auto_NL_manufactum.nl_w9l",
			0,
			"^https?://(www\\.)?manufactum\\.nl/",
			10,
			[],
			[{ "e": 892 }],
			[{ "v": 892 }],
			[{ "wait": 500 }, { "c": 892 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 892
			}],
			{}
		],
		[
			1,
			"auto_NL_matrassencheck.nl_cap",
			0,
			"^https?://(www\\.)?matrassencheck\\.nl/",
			10,
			[],
			[{ "e": 1121 }],
			[{ "v": 1121 }],
			[{ "wait": 500 }, { "c": 1121 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1121
			}],
			{}
		],
		[
			1,
			"auto_NL_mijn.simyo.nl_xm9",
			0,
			"^https?://(www\\.)?mijn\\.simyo\\.nl/",
			10,
			[],
			[{ "e": 1122 }],
			[{ "v": 1122 }],
			[{ "wait": 500 }, { "c": 1122 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1122
			}],
			{}
		],
		[
			1,
			"auto_NL_mijngelderland.nl_rkj",
			0,
			"^https?://(www\\.)?mijngelderland\\.nl/",
			10,
			[],
			[{ "e": 1123 }],
			[{ "v": 1123 }],
			[{ "wait": 500 }, { "c": 1123 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1123
			}],
			{}
		],
		[
			1,
			"auto_NL_milieucentraal.nl_p66",
			0,
			"^https?://(www\\.)?milieucentraal\\.nl/",
			10,
			[],
			[{ "e": 1124 }],
			[{ "v": 1124 }],
			[{ "wait": 500 }, { "c": 1124 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1124
			}],
			{}
		],
		[
			1,
			"auto_NL_muziekgebouw.nl_cob",
			0,
			"^https?://(www\\.)?muziekgebouw\\.nl/",
			10,
			[],
			[{ "e": 1094 }],
			[{ "v": 1094 }],
			[{ "wait": 500 }, { "c": 1094 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1094
			}],
			{}
		],
		[
			1,
			"auto_NL_nec-nijmegen.nl_04y",
			0,
			"^https?://(www\\.)?nec-nijmegen\\.nl/",
			10,
			[],
			[{ "e": 1125 }],
			[{ "v": 1125 }],
			[{ "wait": 500 }, { "c": 1125 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1125
			}],
			{}
		],
		[
			1,
			"auto_NL_nederlandseloterij.nl_b60",
			0,
			"^https?://(www\\.)?nederlandseloterij\\.nl/",
			10,
			[],
			[{ "e": 1126 }],
			[{ "v": 1126 }],
			[{ "wait": 500 }, { "c": 1126 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1126
			}],
			{}
		],
		[
			1,
			"auto_NL_nl.spankbang.com_bnh_+1",
			0,
			"^https?://(www\\.)?nl\\.spankbang\\.com/|^https?://(www\\.)?spankbang\\.com/",
			10,
			[],
			[{ "e": 1127 }],
			[{ "v": 1127 }],
			[{ "wait": 500 }, { "c": 1127 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1127
			}],
			{}
		],
		[
			1,
			"auto_NL_orpheus.nl_cxu",
			0,
			"^https?://(www\\.)?orpheus\\.nl/",
			10,
			[],
			[{ "e": 1094 }],
			[{ "v": 1094 }],
			[{ "wait": 500 }, { "c": 1094 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1094
			}],
			{}
		],
		[
			1,
			"auto_NL_partnerplatform.bol.com_k9h",
			0,
			"^https?://(www\\.)?partnerplatform\\.bol\\.com/",
			10,
			[],
			[{ "e": 1128 }],
			[{ "v": 1128 }],
			[{ "wait": 500 }, { "c": 1128 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1128
			}],
			{}
		],
		[
			1,
			"auto_NL_persportaal.anp.nl_o32",
			0,
			"^https?://(www\\.)?persportaal\\.anp\\.nl/",
			10,
			[],
			[{ "e": 1129 }],
			[{ "v": 1129 }],
			[{ "wait": 500 }, { "c": 1129 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1129
			}],
			{}
		],
		[
			1,
			"auto_NL_planteenolijfboom.nl_pdo",
			0,
			"^https?://(www\\.)?planteenolijfboom\\.nl/",
			10,
			[],
			[{ "e": 1130 }],
			[{ "v": 1130 }],
			[{ "wait": 500 }, { "c": 1130 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1130
			}],
			{}
		],
		[
			1,
			"auto_NL_psv.nl_2pt",
			0,
			"^https?://(www\\.)?psv\\.nl/",
			10,
			[],
			[{ "e": 1131 }],
			[{ "v": 1131 }],
			[{ "wait": 500 }, { "c": 1131 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1131
			}],
			{}
		],
		[
			1,
			"auto_NL_sanitairkamer.nl_vig",
			0,
			"^https?://(www\\.)?sanitairkamer\\.nl/",
			10,
			[],
			[{ "e": 1132 }],
			[{ "v": 1132 }],
			[{ "wait": 500 }, { "c": 1132 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1132
			}],
			{}
		],
		[
			1,
			"auto_NL_schaapcitroen.nl_6v0",
			0,
			"^https?://(www\\.)?schaapcitroen\\.nl/",
			10,
			[],
			[{ "e": 1133 }],
			[{ "v": 1133 }],
			[{ "wait": 500 }, { "c": 1133 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1133
			}],
			{}
		],
		[
			1,
			"auto_NL_scouting.nl_ue4",
			0,
			"^https?://(www\\.)?scouting\\.nl/",
			10,
			[],
			[{ "e": 1134 }],
			[{ "v": 1134 }],
			[{ "wait": 500 }, { "c": 1134 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1134
			}],
			{}
		],
		[
			1,
			"auto_NL_slachtofferhulp.nl_4m4",
			0,
			"^https?://(www\\.)?slachtofferhulp\\.nl/",
			10,
			[],
			[{ "e": 1135 }],
			[{ "v": 1135 }],
			[{ "wait": 500 }, { "c": 1135 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1135
			}],
			{}
		],
		[
			1,
			"auto_NL_sprinklr.co_3ww",
			0,
			"^https?://(www\\.)?sprinklr\\.co/",
			10,
			[],
			[{ "e": 821 }],
			[{ "v": 821 }],
			[{ "wait": 500 }, { "c": 821 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 821
			}],
			{}
		],
		[
			1,
			"auto_NL_stadsschouwburg-utrecht.nl_tcz",
			0,
			"^https?://(www\\.)?stadsschouwburg-utrecht\\.nl/",
			10,
			[],
			[{ "e": 1136 }],
			[{ "v": 1136 }],
			[{ "wait": 500 }, { "c": 1136 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1136
			}],
			{}
		],
		[
			1,
			"auto_NL_stedelijkmuseumschiedam.nl_y6j",
			0,
			"^https?://(www\\.)?stedelijkmuseumschiedam\\.nl/",
			10,
			[],
			[{ "e": 1028 }],
			[{ "v": 1028 }],
			[{ "wait": 500 }, { "c": 1028 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1028
			}],
			{}
		],
		[
			1,
			"auto_NL_texelsecourant.nl_v4b",
			0,
			"^https?://(www\\.)?texelsecourant\\.nl/",
			10,
			[],
			[{ "e": 1137 }],
			[{ "v": 1137 }],
			[{ "wait": 500 }, { "c": 1137 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1137
			}],
			{}
		],
		[
			1,
			"auto_NL_ticketswap.com_d3j",
			0,
			"^https?://(www\\.)?ticketswap\\.com/",
			10,
			[],
			[{ "e": 1138 }],
			[{ "v": 1138 }],
			[{ "wait": 500 }, { "c": 1138 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1138
			}],
			{}
		],
		[
			1,
			"auto_NL_uit.inapeldoorn.nl_gt5",
			0,
			"^https?://(www\\.)?uit\\.inapeldoorn\\.nl/",
			10,
			[],
			[{ "e": 1139 }],
			[{ "v": 1139 }],
			[{ "wait": 500 }, { "c": 1139 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1139
			}],
			{}
		],
		[
			1,
			"auto_NL_vlaanderen.be_c8w",
			0,
			"^https?://(www\\.)?vlaanderen\\.be/",
			10,
			[],
			[{ "e": 1140 }],
			[{ "v": 1140 }],
			[{ "wait": 500 }, { "c": 1140 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1140
			}],
			{}
		],
		[
			1,
			"auto_NL_vvaa.nl_avs",
			0,
			"^https?://(www\\.)?vvaa\\.nl/",
			10,
			[],
			[{ "e": 1141 }],
			[{ "v": 1141 }],
			[{ "wait": 500 }, { "c": 1141 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1141
			}],
			{}
		],
		[
			1,
			"auto_NL_werkenbijdefensie.nl_0cv",
			0,
			"^https?://(www\\.)?werkenbijdefensie\\.nl/",
			10,
			[],
			[{ "e": 1142 }],
			[{ "v": 1142 }],
			[{ "wait": 500 }, { "c": 1142 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1142
			}],
			{}
		],
		[
			1,
			"auto_NL_winkelstraat.nl_hxj",
			0,
			"^https?://(www\\.)?winkelstraat\\.nl/",
			10,
			[],
			[{ "e": 1143 }],
			[{ "v": 1143 }],
			[{ "wait": 500 }, { "c": 1143 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1143
			}],
			{}
		],
		[
			1,
			"auto_NL_woonnetrijnmond.nl_itj",
			0,
			"^https?://(www\\.)?woonnetrijnmond\\.nl/",
			10,
			[],
			[{ "e": 1144 }],
			[{ "v": 1144 }],
			[{ "wait": 500 }, { "c": 1144 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1144
			}],
			{}
		],
		[
			1,
			"auto_NL_zuidas.nl_abm",
			0,
			"^https?://(www\\.)?zuidas\\.nl/",
			10,
			[],
			[{ "e": 1145 }],
			[{ "v": 1145 }],
			[{ "wait": 500 }, { "c": 1145 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1145
			}],
			{}
		],
		[
			1,
			"auto_NO_apxml.com_l6n",
			0,
			"^https?://(www\\.)?apxml\\.com/",
			10,
			[],
			[{ "e": 1146 }],
			[{ "v": 1146 }],
			[{ "wait": 500 }, { "c": 1146 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1146
			}],
			{}
		],
		[
			1,
			"auto_NO_arbetsformedlingen.se_wxz",
			0,
			"^https?://(www\\.)?arbetsformedlingen\\.se/",
			10,
			[],
			[{ "e": 1147 }],
			[{ "v": 1147 }],
			[{ "wait": 500 }, { "c": 1147 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1147
			}],
			{}
		],
		[
			1,
			"auto_NO_arronet.se_vo5",
			0,
			"^https?://(www\\.)?arronet\\.se/",
			10,
			[],
			[{ "e": 1148 }],
			[{ "v": 1148 }],
			[{ "wait": 500 }, { "c": 1148 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1148
			}],
			{}
		],
		[
			1,
			"auto_NO_avanza.se_b5b",
			0,
			"^https?://(www\\.)?avanza\\.se/",
			10,
			[],
			[{ "e": 1149 }],
			[{ "v": 1149 }],
			[{ "wait": 500 }, { "c": 1149 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1149
			}],
			{}
		],
		[
			1,
			"auto_NO_avidafinance.com_jwc",
			0,
			"^https?://(www\\.)?avidafinance\\.com/",
			10,
			[],
			[{ "e": 1150 }],
			[{ "v": 1150 }],
			[{ "wait": 500 }, { "c": 1150 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1150
			}],
			{}
		],
		[
			1,
			"auto_NO_beducated.com_jjt",
			0,
			"^https?://(www\\.)?beducated\\.com/",
			10,
			[],
			[{ "e": 1151 }],
			[{ "v": 1151 }],
			[{ "wait": 500 }, { "c": 1151 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1151
			}],
			{}
		],
		[
			1,
			"auto_NO_dsa.no_w2b",
			0,
			"^https?://(www\\.)?dsa\\.no/",
			10,
			[],
			[{ "e": 1152 }],
			[{ "v": 1152 }],
			[{ "wait": 500 }, { "c": 1152 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1152
			}],
			{}
		],
		[
			1,
			"auto_NO_goteborg.com_q7v",
			0,
			"^https?://(www\\.)?goteborg\\.com/",
			10,
			[],
			[{ "e": 1153 }],
			[{ "v": 1153 }],
			[{ "wait": 500 }, { "c": 1153 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1153
			}],
			{}
		],
		[
			1,
			"auto_NO_hacksmith.store_49t",
			0,
			"^https?://(www\\.)?hacksmith\\.store/",
			10,
			[],
			[{ "e": 1154 }],
			[{ "v": 1154 }],
			[{ "wait": 500 }, { "c": 1154 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1154
			}],
			{}
		],
		[
			1,
			"auto_NO_hotelfinse1222.no_zpm",
			0,
			"^https?://(www\\.)?hotelfinse1222\\.no/",
			10,
			[],
			[{ "e": 1130 }],
			[{ "v": 1130 }],
			[{ "wait": 500 }, { "c": 1130 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1130
			}],
			{}
		],
		[
			1,
			"auto_NO_jaksta.com_own",
			0,
			"^https?://(www\\.)?jaksta\\.com/",
			10,
			[],
			[{ "e": 1155 }],
			[{ "v": 1155 }],
			[{ "wait": 500 }, { "c": 1155 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1155
			}],
			{}
		],
		[
			1,
			"auto_NO_ledigajobb.se_tx3",
			0,
			"^https?://(www\\.)?ledigajobb\\.se/",
			10,
			[],
			[{ "e": 1156 }],
			[{ "v": 1156 }],
			[{ "wait": 500 }, { "c": 1156 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1156
			}],
			{}
		],
		[
			1,
			"auto_NO_liseberg.se_qy4",
			0,
			"^https?://(www\\.)?liseberg\\.se/",
			10,
			[],
			[{ "e": 1157 }],
			[{ "v": 1157 }],
			[{ "wait": 500 }, { "c": 1157 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1157
			}],
			{}
		],
		[
			1,
			"auto_NO_nordnet.se_2en",
			0,
			"^https?://(www\\.)?nordnet\\.se/",
			10,
			[],
			[{ "e": 1158 }],
			[{ "v": 1158 }],
			[{ "wait": 500 }, { "c": 1158 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1158
			}],
			{}
		],
		[
			1,
			"auto_NO_pcsx2.net_grl",
			0,
			"^https?://(www\\.)?pcsx2\\.net/",
			10,
			[],
			[{ "e": 1159 }],
			[{ "v": 1159 }],
			[{ "wait": 500 }, { "c": 1159 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1159
			}],
			{}
		],
		[
			1,
			"auto_NO_regeringen.se_uhv",
			0,
			"^https?://(www\\.)?regeringen\\.se/",
			10,
			[],
			[{ "e": 1160 }],
			[{ "v": 1160 }],
			[{ "wait": 500 }, { "c": 1160 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1160
			}],
			{}
		],
		[
			1,
			"auto_NO_saseurobonusmastercard.se_oqd",
			0,
			"^https?://(www\\.)?saseurobonusmastercard\\.se/",
			10,
			[],
			[{ "e": 1161 }],
			[{ "v": 1161 }],
			[{ "wait": 500 }, { "c": 1161 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1161
			}],
			{}
		],
		[
			1,
			"auto_NO_sverigesradio.se_a00",
			0,
			"^https?://(www\\.)?sverigesradio\\.se/",
			10,
			[],
			[{ "e": 1162 }],
			[{ "v": 1162 }],
			[{ "wait": 500 }, { "c": 1162 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1162
			}],
			{}
		],
		[
			1,
			"auto_NO_svtplay.se_ujn",
			0,
			"^https?://(www\\.)?svtplay\\.se/",
			10,
			[],
			[{ "e": 1163 }],
			[{ "v": 1163 }],
			[{ "wait": 500 }, { "c": 1163 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1163
			}],
			{}
		],
		[
			1,
			"auto_NO_swedbank.se_63w",
			0,
			"^https?://(www\\.)?swedbank\\.se/",
			10,
			[],
			[{ "e": 1164 }],
			[{ "v": 1164 }],
			[{ "wait": 500 }, { "c": 1164 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1164
			}],
			{}
		],
		[
			1,
			"auto_NO_tingstad.com_z23",
			0,
			"^https?://(www\\.)?tingstad\\.com/",
			10,
			[],
			[{ "e": 1165 }],
			[{ "v": 1165 }],
			[{ "wait": 500 }, { "c": 1165 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1165
			}],
			{}
		],
		[
			1,
			"auto_NO_truecaller.com_sdg",
			0,
			"^https?://(www\\.)?truecaller\\.com/",
			10,
			[],
			[{ "e": 1166 }],
			[{ "v": 1166 }],
			[{ "wait": 500 }, { "c": 1166 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1166
			}],
			{}
		],
		[
			1,
			"auto_NO_uu.se_eea",
			0,
			"^https?://(www\\.)?uu\\.se/",
			10,
			[],
			[{ "e": 1167 }],
			[{ "v": 1167 }],
			[{ "wait": 500 }, { "c": 1167 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1167
			}],
			{}
		],
		[
			1,
			"auto_NO_webank.it_6rx",
			0,
			"^https?://(www\\.)?webank\\.it/",
			10,
			[],
			[{ "e": 1168 }],
			[{ "v": 1168 }],
			[{ "wait": 500 }, { "c": 1168 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1168
			}],
			{}
		],
		[
			1,
			"auto_NO_xrealgirl.com_r42",
			0,
			"^https?://(www\\.)?xrealgirl\\.com/",
			10,
			[],
			[{ "e": 1169 }],
			[{ "v": 1169 }],
			[{ "wait": 500 }, { "c": 1169 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1169
			}],
			{}
		],
		[
			1,
			"auto_US_amazon.jobs_0",
			0,
			"^https?://(www\\.)?amazon\\.jobs/",
			10,
			[],
			[{ "e": 1170 }],
			[{ "v": 1170 }],
			[{ "wait": 500 }, { "c": 1170 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1170
			}],
			{}
		],
		[
			1,
			"auto_US_amsoil.com_fgr",
			0,
			"^https?://(www\\.)?amsoil\\.com/",
			10,
			[],
			[{ "e": 1171 }],
			[{ "v": 1171 }],
			[{ "c": 1171 }],
			[],
			{}
		],
		[
			1,
			"auto_US_balsamhill.com_oxq",
			0,
			"^https?://(www\\.)?balsamhill\\.com/",
			10,
			[],
			[{ "e": 1172 }],
			[{ "v": 1172 }],
			[{ "wait": 500 }, { "c": 1172 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1172
			}],
			{}
		],
		[
			1,
			"auto_US_computerworld.com_0_+1",
			0,
			"^https?://(www\\.)?computerworld\\.com/|^https?://(www\\.)?csoonline\\.com/",
			10,
			[],
			[{ "e": 1173 }],
			[{ "v": 1173 }],
			[{
				"text": "Do not accept",
				"c": 1173
			}],
			[],
			{}
		],
		[
			1,
			"auto_US_coupons.slickdeals.net_wya",
			0,
			"^https?://(www\\.)?coupons\\.slickdeals\\.net/",
			10,
			[],
			[{ "e": 1174 }],
			[{ "v": 1174 }],
			[{ "wait": 500 }, { "c": 1174 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1174
			}],
			{}
		],
		[
			1,
			"auto_US_deezer.com_0",
			0,
			"^https?://(www\\.)?deezer\\.com/",
			10,
			[],
			[{ "e": 1175 }],
			[{ "v": 1175 }],
			[{
				"text": "Refuse",
				"c": 1175
			}],
			[],
			{}
		],
		[
			1,
			"auto_US_emagine-entertainment.com_0",
			0,
			"^https?://(www\\.)?emagine-entertainment\\.com/",
			10,
			[],
			[{ "e": 1176 }],
			[{ "v": 1176 }],
			[{ "wait": 500 }, { "c": 1176 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1176
			}],
			{}
		],
		[
			1,
			"auto_US_fawesome.tv_gjx",
			0,
			"^https?://(www\\.)?fawesome\\.tv/",
			10,
			[],
			[{ "e": 1177 }],
			[{ "v": 1177 }],
			[{ "wait": 500 }, { "c": 1177 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1177
			}],
			{}
		],
		[
			1,
			"auto_US_forum.affinity.serif.com_0",
			0,
			"^https?://(www\\.)?forum\\.affinity\\.serif\\.com/",
			10,
			[],
			[{ "e": 1178 }],
			[{ "v": 1178 }],
			[{
				"text": "\xA0Reject Cookies",
				"c": 1178
			}],
			[],
			{}
		],
		[
			1,
			"auto_US_forum.prusa3d.com_0",
			0,
			"^https?://(www\\.)?forum\\.prusa3d\\.com/",
			10,
			[],
			[{ "e": 1179 }],
			[{ "v": 1179 }],
			[{
				"text": "Reject All",
				"c": 1179
			}],
			[],
			{}
		],
		[
			1,
			"auto_US_greenpan.us_cko",
			0,
			"^https?://(www\\.)?greenpan\\.us/",
			10,
			[],
			[{ "e": 821 }],
			[{ "v": 821 }],
			[{ "wait": 500 }, { "c": 821 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 821
			}],
			{}
		],
		[
			1,
			"auto_US_interactivebrokers.com_0",
			0,
			"^https?://(www\\.)?interactivebrokers\\.com/",
			10,
			[],
			[{ "e": 1180 }],
			[{ "v": 1180 }],
			[{
				"text": "Reject All Cookies",
				"c": 1180
			}],
			[],
			{}
		],
		[
			1,
			"auto_US_ligonier.org_0",
			0,
			"^https?://(www\\.)?ligonier\\.org/",
			10,
			[],
			[{ "e": 1181 }],
			[{ "v": 1181 }],
			[{
				"text": "Strictly Necessary",
				"c": 1181
			}],
			[],
			{}
		],
		[
			1,
			"auto_US_lilly.com_lh6_+1",
			0,
			"^https?://(www\\.)?lilly\\.com/|^https?://(www\\.)?zepbound\\.lilly\\.com/",
			10,
			[],
			[{ "e": 1182 }],
			[{ "v": 1182 }],
			[{ "wait": 500 }, { "c": 1182 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1182
			}],
			{}
		],
		[
			1,
			"auto_US_mixedbread.ai_tge",
			0,
			"^https?://(www\\.)?mixedbread\\.com/",
			10,
			[],
			[{ "e": 1146 }],
			[{ "v": 1146 }],
			[{ "wait": 500 }, { "c": 1146 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1146
			}],
			{}
		],
		[
			1,
			"auto_US_musicnotes.com_0",
			0,
			"^https?://(www\\.)?musicnotes\\.com/",
			10,
			[],
			[{ "exists": ["#polaris-css-lockdown-container", ".polaris-consent-widget"] }],
			[{ "visible": ["#polaris-css-lockdown-container", ".polaris-consent-widget"] }],
			[{
				"if": { "exists": ["#polaris-css-lockdown-container", "[data-testid='oneClickOptOutLink']"] },
				"then": [{ "waitForThenClick": ["#polaris-css-lockdown-container", "[data-testid='oneClickOptOutLink']"] }],
				"else": [{ "hide": ["#polaris-css-lockdown-container", ".polaris-consent-widget"] }]
			}],
			[],
			{}
		],
		[
			1,
			"auto_US_newmedicare.com_6jp",
			0,
			"^https?://(www\\.)?newmedicare\\.com/",
			10,
			[],
			[{ "e": 1183 }],
			[{ "v": 1183 }],
			[{ "wait": 500 }, { "c": 1183 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1183
			}],
			{}
		],
		[
			1,
			"auto_US_newsbreak.com_0",
			0,
			"^https?://(www\\.)?newsbreak\\.com/",
			10,
			[],
			[{ "e": 1184 }],
			[{ "v": 1184 }],
			[{ "c": 1184 }],
			[],
			{}
		],
		[
			1,
			"auto_US_peptidesciences.com_0",
			0,
			"^https?://(www\\.)?peptidesciences\\.com/",
			10,
			[],
			[{ "e": 1185 }],
			[{ "v": 1185 }],
			[{ "wait": 500 }, { "c": 1185 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1185
			}],
			{}
		],
		[
			1,
			"auto_US_semrush.com_0",
			0,
			"^https?://(www\\.)?semrush\\.com/",
			10,
			[],
			[{ "e": 1186 }],
			[{ "v": 1186 }],
			[{ "wait": 500 }, { "c": 1186 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1186
			}],
			{}
		],
		[
			1,
			"auto_US_sso.passport.yandex.ru_0_+5",
			0,
			"^https?://(www\\.)?sso\\.passport\\.yandex\\.ru/|^https?://(www\\.)?translate\\.yandex\\.com/|^https?://(www\\.)?tv\\.yandex\\.com/|^https?://(www\\.)?ya\\.ru/|^https?://(www\\.)?yandex\\.com\\.tr/|^https?://(www\\.)?yandex\\.com/",
			10,
			[],
			[{ "e": 1187 }],
			[{ "v": 1187 }],
			[{
				"text": "Allow essential cookies",
				"c": 1187
			}],
			[],
			{}
		],
		[
			1,
			"auto_US_truecaller.com_0",
			0,
			"^https?://(www\\.)?truecaller\\.com/",
			10,
			[],
			[{ "e": 1188 }],
			[{ "v": 1188 }],
			[{
				"text": "Accept Necessary Cookies",
				"c": 1188
			}],
			[],
			{}
		],
		[
			1,
			"auto_US_weingartz.com_aso",
			0,
			"^https?://(www\\.)?weingartz\\.com/",
			10,
			[],
			[{ "e": 1189 }],
			[{ "v": 1189 }],
			[{ "c": 1189 }],
			[],
			{}
		],
		[
			1,
			"bahn-de",
			0,
			"^https://(www\\.)?bahn\\.de/",
			10,
			[],
			[{ "exists": ["body > div:first-child", "#consent-layer"] }],
			[{ "visible": ["body > div:first-child", "#consent-layer"] }],
			[{ "waitForThenClick": ["body > div:first-child", "#consent-layer .js-accept-essential-cookies"] }],
			[{ "eval": "EVAL_BAHN_TEST" }],
			{ "intermediate": false }
		],
		[
			1,
			"bandcamp.com",
			2,
			"^https://([a-z0-9-]+\\.)?bandcamp\\.com",
			22,
			[],
			[{ "e": 1190 }],
			[{ "v": 1191 }],
			[{ "c": 1192 }],
			[{ "cc": 1193 }],
			{}
		],
		[
			1,
			"bbc.com",
			2,
			"^https://(www\\.)?bbc\\.com",
			22,
			[1194],
			[{ "e": 1195 }],
			[{ "v": 1195 }],
			[{ "c": 1196 }],
			[{ "cc": 1197 }],
			{}
		],
		[
			1,
			"canyon.com",
			2,
			"^https://www\\.canyon\\.com/",
			22,
			[1198],
			[{ "e": 1198 }],
			[{ "v": 1198 }],
			[{ "k": 1199 }, { "c": 1200 }],
			[],
			{}
		],
		[
			1,
			"channel4.com",
			2,
			"^https?://(www\\.)?channel4\\.com/",
			10,
			[511],
			[{ "e": 511 }],
			[{ "v": 1201 }],
			[{ "c": 1202 }],
			[{ "cc": 1203 }],
			{}
		],
		[
			1,
			"chatgpt",
			0,
			"^https://(www\\.)?chatgpt\\.com/",
			10,
			[],
			[{ "e": 1204 }],
			[{ "v": 1204 }],
			[{ "wait": 500 }, { "c": 1204 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1204
			}],
			{}
		],
		[
			1,
			"clustrmaps.com",
			1,
			"^https://(www\\.)?clustrmaps\\.com/",
			22,
			[1205],
			[{ "e": 1205 }],
			[{ "v": 1205 }],
			[{ "h": 1205 }],
			[],
			{}
		],
		[
			1,
			"copilot-microsoft",
			0,
			"^https://copilot\\.microsoft\\.com/",
			10,
			[9],
			[{ "e": 9 }],
			[{ "v": 9 }],
			[{ "c": 1206 }],
			[{ "cc": 1207 }],
			{}
		],
		[
			1,
			"csu-landtag-de",
			2,
			"^https://(www\\.|)?csu-landtag\\.de",
			22,
			[1208],
			[{ "e": 1208 }],
			[{ "v": 1208 }],
			[{ "k": 1209 }],
			[],
			{}
		],
		[
			1,
			"dailymotion.com",
			2,
			"^https://(www\\.)?dailymotion\\.com/",
			22,
			[1210],
			[{ "e": 1211 }],
			[{ "v": 1212 }],
			[{ "c": 1213 }],
			[{ "cc": 1214 }],
			{}
		],
		[
			1,
			"delta.com",
			1,
			"^https://www\\.delta\\.com/",
			22,
			[1215],
			[{ "e": 1216 }],
			[{ "v": 1216 }],
			[{ "h": 1216 }],
			[],
			{}
		],
		[
			1,
			"depop",
			0,
			"^https://(www\\.)?depop\\.com/",
			22,
			[1217],
			[{ "e": 1218 }],
			[{ "v": 1218 }],
			[
				{ "c": 1219 },
				{
					"all": true,
					"optional": true,
					"k": 1220
				},
				{ "c": 1221 }
			],
			[{ "cc": 1222 }],
			{}
		],
		[
			1,
			"dji",
			0,
			"^https://(\\w+\\.)+dji\\.com/",
			22,
			[1223],
			[{ "e": 1224 }],
			[{ "e": 1223 }],
			[{ "c": 1225 }],
			[{ "cc": 1226 }],
			{}
		],
		[
			1,
			"dndbeyond",
			2,
			"^https://(www\\.)?dndbeyond\\.com/",
			22,
			[1227],
			[{ "e": 1227 }],
			[{ "v": 1227 }],
			[{ "c": 1228 }],
			[{ "cc": 1229 }],
			{}
		],
		[
			1,
			"ebay",
			0,
			"^https://(www\\.)?ebay\\.([.a-z]+)/",
			10,
			[1230],
			[{ "e": 1230 }],
			[{ "v": 1230 }],
			[{ "c": 1231 }],
			[],
			{}
		],
		[
			1,
			"ecosia",
			2,
			"^https://www\\.ecosia\\.org/",
			22,
			[1232],
			[{ "e": 1233 }],
			[{ "v": 1233 }],
			[{ "c": 1234 }],
			[],
			{}
		],
		[
			1,
			"edpb-edps",
			2,
			"^https://(www\\.)?(edpb|edps)\\.europa\\.eu/",
			22,
			[1235],
			[{ "e": 1236 }],
			[{ "v": 1235 }],
			[{ "c": 1236 }],
			[{ "cc": 1237 }],
			{}
		],
		[
			1,
			"ef-ccpa",
			0,
			"^https://(www\\.)?eforms\\.com",
			22,
			[1238],
			[{ "e": 1238 }],
			[{ "v": 1238 }],
			[{ "c": 1239 }],
			[],
			{}
		],
		[
			1,
			"escaparium.ca",
			2,
			"^https://(www\\.)?escaparium\\.ca/",
			22,
			[],
			[{ "e": 1240 }],
			[{ "v": 1241 }],
			[{ "c": 1242 }, { "c": 1243 }],
			[{ "cc": 1244 }],
			{}
		],
		[
			1,
			"espace-personnel.agirc-arrco.fr",
			2,
			"^https://espace-personnel\\.agirc-arrco\\.fr/",
			22,
			[1245],
			[{ "e": 1246 }],
			[{ "v": 1246 }],
			[{ "c": 1247 }],
			[],
			{}
		],
		[
			1,
			"europa-eu",
			2,
			"^https://([a-z\\.]*\\.)?europa\\.eu/",
			22,
			[511],
			[{ "e": 1248 }],
			[{ "v": 1248 }],
			[{
				"c": 1249,
				"h": 1248
			}],
			[],
			{}
		],
		[
			1,
			"facebook",
			2,
			"^https://(www\\.)?facebook\\.com/",
			22,
			[],
			[{ "e": 1250 }],
			[{ "v": 1250 }],
			[{ "c": 1250 }, {
				"check": "none",
				"wv": 1250
			}],
			[],
			{}
		],
		[
			1,
			"facebook-mobile",
			2,
			"^https://(m|mbasic)\\.facebook\\.com/",
			22,
			[1251],
			[{ "e": 1251 }],
			[{ "v": 1251 }],
			[{ "c": 1252 }, {
				"check": "none",
				"wv": 1251
			}],
			[],
			{}
		],
		[
			1,
			"financestrategists.com",
			1,
			"^https://(www\\.)?financestrategists\\.com/",
			22,
			[1253],
			[{ "e": 1253 }],
			[{ "v": 1253 }],
			[{ "h": 1253 }],
			[],
			{}
		],
		[
			1,
			"geeks-for-geeks",
			1,
			"^https://www\\.geeksforgeeks\\.org/",
			22,
			[1254],
			[{ "e": 1254 }],
			[{ "v": 1254 }],
			[{ "h": 1254 }],
			[],
			{}
		],
		[
			1,
			"geni.com",
			1,
			"^https://(www\\.)?geni\\.com/",
			22,
			[1255],
			[{ "e": 1255 }],
			[{ "v": 1255 }],
			[{ "h": 1255 }],
			[],
			{}
		],
		[
			1,
			"glastonburyfestivals",
			0,
			"^https://(\\w+\\.)?glastonburyfestivals\\.co\\.uk/",
			22,
			[1256],
			[{ "e": 1257 }],
			[{ "v": 1257 }],
			[{ "c": 1258 }],
			[],
			{}
		],
		[
			1,
			"groundnews",
			0,
			"^https://(www\\.)?ground\\.news/",
			22,
			[],
			[{ "e": 1259 }],
			[{ "v": 1259 }],
			[
				{ "waitForThenClick": [".fixed:has([data-testid=closeCookieBanner])", "xpath///button[contains(., 'Manage cookies')]"] },
				{ "waitFor": ["[data-testid=modal]", "xpath///span[contains(., 'Essential cookies')]"] },
				{
					"click": ["[data-testid=modal]", "xpath///button[contains(., 'On')]"],
					"all": true,
					"optional": true
				},
				{ "waitForThenClick": ["[data-testid=modal]", "xpath///button[contains(., 'Save & reload')]"] }
			],
			[],
			{}
		],
		[
			1,
			"hashicorp",
			2,
			"^https://[a-z]*\\.hashicorp\\.com/",
			22,
			[1260],
			[{ "e": 1260 }],
			[{ "v": 1260 }],
			[{ "c": 1261 }, { "c": 1262 }],
			[],
			{}
		],
		[
			1,
			"hearst-us",
			1,
			"^https://(www\\.)?(menshealth|womenshealthmag|cosmopolitan|esquire|elle|elledecor|harpersbazaar|goodhousekeeping|popularmechanics|caranddriver|delish|veranda|roadandtrack|bestproducts|countryliving|housebeautiful|bicycling|biography|pitchfork|prevention|runnersworld|thepioneerwoman|townandcountrymag)\\.com/",
			10,
			[1263],
			[{ "e": 1263 }],
			[{
				"check": "any",
				"v": 1263
			}],
			[{ "h": 1263 }],
			[{
				"check": "none",
				"v": 1263
			}],
			{}
		],
		[
			1,
			"hetzner.com",
			2,
			"^https://www\\.hetzner\\.com/",
			22,
			[1264],
			[{ "e": 1264 }],
			[{ "v": 1264 }],
			[{ "k": 1265 }],
			[],
			{}
		],
		[
			1,
			"imdb",
			0,
			"^https://(www\\.)?(m\\.)?imdb.com/",
			22,
			[1260],
			[{ "e": 1260 }],
			[{ "v": 1260 }],
			[{ "c": 1266 }],
			[],
			{}
		],
		[
			1,
			"instagram",
			2,
			"^https://www\\.instagram\\.com/",
			22,
			[],
			[{ "e": 1267 }],
			[{ "v": 1267 }],
			[{ "c": 1268 }, { "wait": 2e3 }],
			[],
			{}
		],
		[
			1,
			"itopvpn.com",
			1,
			"^https://(www\\.)?itopvpn.com/",
			22,
			[],
			[{ "e": 1269 }],
			[{ "e": 1269 }],
			[{ "h": 1269 }],
			[],
			{}
		],
		[
			1,
			"jdsports",
			2,
			"^https://(www|m)\\.jdsports\\.",
			22,
			[1270],
			[{ "e": 1270 }],
			[{ "v": 1270 }],
			[{
				"if": { "e": 1271 },
				"then": [{ "h": 1271 }],
				"else": [{ "c": 1272 }, { "c": 1273 }]
			}],
			[],
			{}
		],
		[
			1,
			"justgiving.com",
			2,
			"^https://(?:[a-z0-9-]+\\.)?justgiving\\.com/",
			22,
			[1274],
			[{ "any": [{ "e": 1274 }, { "e": 1275 }] }],
			[{ "v": 1274 }, { "e": 1276 }],
			[{ "c": 1276 }],
			[{ "wait": 500 }, { "cc": 1277 }],
			{}
		],
		[
			1,
			"kickstarter.com",
			2,
			"^https?://(www\\.)?kickstarter\\.com/",
			22,
			[1278],
			[{ "e": 1279 }],
			[{ "v": 1279 }],
			[{ "c": 1280 }],
			[{
				"check": "none",
				"v": 1279
			}],
			{}
		],
		[
			1,
			"kleinanzeigen-de",
			2,
			"^https?://(www\\.)?kleinanzeigen\\.de",
			22,
			[1281],
			[{ "e": 1282 }],
			[{ "v": 1282 }],
			[{ "k": 1282 }],
			[],
			{}
		],
		[
			1,
			"leafly",
			1,
			"^https://(www\\.)?leafly\\.com/",
			22,
			[],
			[{ "e": 1283 }],
			[{ "v": 1283 }],
			[{ "h": 1283 }],
			[],
			{}
		],
		[
			1,
			"medium",
			1,
			"^https://([a-z0-9-]+\\.)?medium\\.com/",
			10,
			[],
			[{ "e": 1284 }],
			[{ "v": 1284 }],
			[{ "h": 1284 }],
			[],
			{}
		],
		[
			1,
			"midway-usa",
			1,
			"^https://www\\.midwayusa\\.com/",
			22,
			[1285],
			[{ "exists": ["div[aria-label=\"Cookie Policy Banner\"]"] }],
			[{ "v": 1285 }],
			[{ "h": 1286 }],
			[],
			{}
		],
		[
			1,
			"nba.com",
			1,
			"^https://(www\\.)?nba\\.com/",
			22,
			[1287],
			[{ "e": 1287 }],
			[{ "v": 1287 }],
			[{ "h": 1287 }],
			[],
			{}
		],
		[
			1,
			"netbeat.de",
			2,
			"^https://(www\\.)?netbeat\\.de/",
			22,
			[1288],
			[{ "e": 1288 }],
			[{ "v": 1288 }],
			[{ "c": 1289 }],
			[],
			{}
		],
		[
			1,
			"nhnieuws",
			2,
			"^https://(www\\.)?nhnieuws\\.nl/",
			22,
			[1290],
			[{ "e": 1290 }],
			[{ "v": 1290 }],
			[{
				"all": true,
				"c": 1291
			}, { "c": 1292 }],
			[{ "eval": "EVAL_NHNIEUWS_TEST" }],
			{}
		],
		[
			1,
			"nike",
			2,
			"^https://(www\\.)?nike\\.com/",
			22,
			[],
			[{ "e": 1293 }],
			[{ "v": 1293 }],
			[{
				"all": true,
				"c": 1294
			}, { "c": 1295 }],
			[],
			{}
		],
		[
			1,
			"nos.nl",
			1,
			"^https://nos\\.nl/",
			22,
			[1296],
			[{ "e": 1296 }],
			[{ "visible": ["ccm-notification"] }],
			[{ "h": 1296 }],
			[],
			{}
		],
		[
			1,
			"nutritionix.com",
			0,
			"^https://(www\\.)?nutritionix\\.com/",
			22,
			[1297],
			[{ "e": 1297 }],
			[{ "v": 1297 }],
			[{ "waitForThenClick": ["gdpr-banner", "xpath///button[contains(., 'Refuse')]"] }],
			[],
			{}
		],
		[
			1,
			"ok",
			0,
			"^https://ok\\.ru/",
			22,
			[1298],
			[{ "e": 1299 }],
			[{ "v": 1299 }],
			[
				{ "w": 1300 },
				{ "wait": 1e3 },
				{ "k": 1300 },
				{ "wv": 1301 },
				{ "wait": 500 },
				{
					"all": true,
					"optional": true,
					"k": 1302
				},
				{ "c": 1303 },
				{
					"check": "none",
					"wv": 1301
				}
			],
			[],
			{}
		],
		[
			1,
			"onlyFans.com",
			2,
			"^https://onlyfans\\.com/",
			22,
			[1304],
			[{ "e": 1304 }],
			[{ "e": 1304 }],
			[{ "k": 1305 }, {
				"if": { "e": 1306 },
				"then": [{
					"all": true,
					"k": 1307
				}, { "k": 1308 }]
			}],
			[],
			{}
		],
		[
			1,
			"openai",
			0,
			"^https://([a-z0-9-]+\\.)?openai\\.com/",
			22,
			[1309],
			[{ "e": 1309 }],
			[{ "v": 1309 }],
			[{ "c": 1310 }],
			[{ "wait": 500 }, { "cc": 1311 }],
			{}
		],
		[
			1,
			"opera.com",
			0,
			"^https?://(www\\.|)?opera\\.com/",
			22,
			[1312],
			[{ "e": 1313 }],
			[{ "v": 1313 }],
			[{
				"all": true,
				"c": 1314
			}, { "c": 1315 }],
			[{ "cc": 1316 }, {
				"negated": true,
				"cc": 1317
			}],
			{}
		],
		[
			1,
			"ourworldindata",
			2,
			"^https://ourworldindata\\.org/",
			22,
			[1318],
			[{ "e": 1318 }],
			[{ "v": 1319 }],
			[{ "c": 1320 }],
			[],
			{}
		],
		[
			1,
			"paychex",
			1,
			"^https://(www\\.)?paychex\\.com/",
			22,
			[1321],
			[{ "e": 1321 }],
			[{ "v": 1321 }],
			[{ "h": 1321 }],
			[],
			{}
		],
		[
			1,
			"pinterest-business",
			2,
			"^https://[a-z]*\\.pinterest\\.com/",
			22,
			[1322],
			[{ "e": 1322 }],
			[{ "v": 1323 }],
			[{ "c": 1324 }],
			[],
			{}
		],
		[
			1,
			"plos",
			0,
			"^https://([.a-zA-Z0-9-]+\\.)?plos\\.org/",
			22,
			[1312],
			[{ "e": 1325 }],
			[{ "v": 1325 }],
			[{
				"all": true,
				"optional": true,
				"k": 1326
			}, { "waitForThenClick": ["#cookie-consent", "xpath///button[contains(., 'Save Selected')]"] }],
			[{ "cc": 1327 }],
			{}
		],
		[
			1,
			"pornhub.com",
			0,
			"^https://(www\\.)?pornhub\\.com/",
			22,
			[1328],
			[{ "e": 1329 }],
			[{ "v": 1329 }],
			[{
				"if": { "e": 1330 },
				"then": [{ "c": 1330 }]
			}, {
				"if": { "e": 1331 },
				"then": [{ "k": 1331 }],
				"else": [{ "c": 1332 }]
			}],
			[],
			{}
		],
		[
			1,
			"postnl",
			0,
			"^https://([a-z]*\\.)?postnl\\.nl/",
			22,
			[1333],
			[{ "e": 1333 }],
			[{ "v": 1333 }],
			[{ "waitForThenClick": ["pnl-cookie-wall-widget", "button.pci-button--secondary"] }],
			[{ "cc": 1334 }],
			{}
		],
		[
			1,
			"povr",
			0,
			"^https://povr\\.com/",
			22,
			[],
			[{ "e": 1335 }],
			[{ "v": 1335 }],
			[{ "h": 1336 }, {
				"if": { "e": 1337 },
				"then": [
					{ "w": 1338 },
					{
						"all": true,
						"optional": true,
						"k": 1339
					},
					{ "c": 1340 },
					{ "eval": "EVAL_POVR_GOBACK" }
				],
				"else": [{ "c": 1341 }]
			}],
			[],
			{}
		],
		[
			1,
			"productz.com",
			2,
			"^https://productz\\.com/",
			22,
			[],
			[{ "e": 1342 }],
			[{ "v": 1342 }],
			[{ "c": 1343 }],
			[],
			{}
		],
		[
			1,
			"raspberrypi.com",
			0,
			"^https://([a-z0-9-]+\\.)?raspberrypi\\.com/",
			10,
			[],
			[{ "e": 1344 }],
			[{ "v": 1344 }],
			[{ "c": 1344 }],
			[{
				"check": "none",
				"v": 1344
			}],
			{}
		],
		[
			1,
			"readly",
			2,
			"^https://([a-z0-9-]+\\.)?readly\\.com/",
			22,
			[1345],
			[{ "e": 1345 }],
			[{ "v": 1345 }],
			[
				{ "c": 1346 },
				{
					"if": { "e": 1347 },
					"then": [{
						"all": true,
						"c": 1347
					}],
					"else": []
				},
				{ "c": 1348 }
			],
			[],
			{}
		],
		[
			1,
			"reddit.com",
			2,
			"^https://www\\.reddit\\.com/",
			22,
			[1349],
			[{ "e": 1350 }],
			[{ "v": 1350 }],
			[{ "c": 1351 }],
			[{ "cc": 1352 }],
			{}
		],
		[
			1,
			"remarkable.com",
			1,
			"^https://(www\\.)?remarkable\\.com/",
			22,
			[1353],
			[{ "e": 1354 }],
			[{ "v": 1354 }],
			[{ "h": 1353 }],
			[],
			{}
		],
		[
			1,
			"roblox",
			0,
			"^https://(www\\.)?roblox\\.com/",
			10,
			[],
			[{ "e": 1355 }],
			[{ "v": 1356 }],
			[{ "c": 1357 }],
			[{ "cc": 1358 }],
			{}
		],
		[
			1,
			"rog-forum.asus.com",
			2,
			"^https://rog-forum\\.asus\\.com/",
			22,
			[1359],
			[{ "e": 1359 }],
			[{ "v": 1359 }],
			[{ "k": 1360 }, { "c": 1361 }],
			[],
			{}
		],
		[
			1,
			"roofingmegastore.co.uk",
			2,
			"^https://(www\\.)?roofingmegastore\\.co\\.uk",
			22,
			[1362],
			[{ "e": 1362 }],
			[{ "v": 1362 }],
			[{ "k": 1363 }, { "c": 1364 }],
			[],
			{}
		],
		[
			1,
			"rt",
			1,
			"^https://(www\\.)?rt\\.com/",
			22,
			[1365],
			[{ "e": 1365 }],
			[{ "v": 1365 }],
			[{ "h": 1365 }],
			[],
			{}
		],
		[
			1,
			"ryanair",
			0,
			"^https://(www\\.)?ryanair\\.com/",
			10,
			[1366],
			[{ "e": 1366 }],
			[{ "v": 1366 }],
			[{ "c": 1367 }],
			[{ "cc": 1368 }],
			{}
		],
		[
			1,
			"samsung.com",
			1,
			"^https://www\\.samsung\\.com/",
			22,
			[1369],
			[{ "e": 1369 }],
			[{ "v": 1369 }],
			[{ "h": 1369 }],
			[],
			{}
		],
		[
			1,
			"schoolhouse-com",
			1,
			"^https://(www\\.)?schoolhouse\\.com/",
			22,
			[1370],
			[{ "e": 1370 }],
			[{ "v": 1370 }],
			[{ "h": 1370 }],
			[],
			{}
		],
		[
			1,
			"shein.com",
			2,
			"^https?://([a-z]+\\.)?shein\\.com/",
			22,
			[1371],
			[{ "e": 1372 }],
			[{ "v": 1372 }],
			[{ "c": 1372 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1372
			}],
			{}
		],
		[
			1,
			"skyscanner",
			0,
			"^https://(www\\.)?skyscanner[\\.a-z]+/",
			10,
			[1355],
			[{ "e": 1373 }],
			[{ "v": 1373 }],
			[{ "c": 1374 }, {
				"check": "none",
				"wv": 1373
			}],
			[{ "eval": "EVAL_SKYSCANNER_TEST" }],
			{}
		],
		[
			1,
			"strato.de",
			2,
			"^https://www\\.strato\\.de/",
			22,
			[1375],
			[{ "e": 1376 }],
			[{ "v": 1376 }],
			[{ "k": 1377 }, { "c": 1378 }],
			[],
			{}
		],
		[
			1,
			"svt.se",
			2,
			"^https://www\\.svt\\.se/",
			22,
			[1379],
			[{ "e": 1379 }],
			[{ "v": 1380 }],
			[{ "c": 1381 }],
			[{ "cc": 1382 }],
			{}
		],
		[
			1,
			"temu",
			2,
			"^https://([a-z0-9-]+\\.)?temu\\.com/",
			22,
			[],
			[{ "e": 1383 }],
			[{ "v": 1383 }],
			[{
				"if": { "e": 1384 },
				"then": [{ "c": 1384 }],
				"else": [{ "c": 1385 }]
			}],
			[],
			{}
		],
		[
			1,
			"tesco",
			0,
			"^https://(www\\.)?tesco\\.com/",
			22,
			[1386],
			[{ "e": 1386 }],
			[{ "v": 1386 }],
			[{ "w": 1387 }, { "c": 1388 }],
			[{ "cc": 1389 }],
			{}
		],
		[
			1,
			"tesla",
			2,
			"^https://(www\\.)?tesla\\.com/",
			10,
			[],
			[{ "e": 1390 }],
			[{ "v": 1390 }],
			[{ "c": 1391 }],
			[{ "cc": 1392 }],
			{}
		],
		[
			1,
			"theinfatuation",
			2,
			"^https://(www\\.)?theinfatuation\\.com",
			10,
			[1393],
			[{ "e": 1394 }],
			[{ "v": 1394 }],
			[{ "c": 1394 }],
			[{
				"check": "none",
				"v": 1393
			}],
			{}
		],
		[
			1,
			"theverge",
			2,
			"^https://(www)?\\.theverge\\.com",
			10,
			[1395],
			[{ "e": 1395 }],
			[{ "v": 1395 }],
			[{ "k": 1396 }],
			[{ "cc": 1397 }],
			{ "intermediate": false }
		],
		[
			1,
			"tinyurl",
			1,
			"^https://tinyurl\\.com/",
			22,
			[1398],
			[{ "e": 1398 }],
			[{ "v": 1398 }],
			[{ "h": 1398 }],
			[],
			{}
		],
		[
			1,
			"track.amazon.com",
			2,
			"^https://[a-z]*\\.amazon\\.",
			22,
			[1399],
			[{ "e": 1400 }],
			[{ "v": 1400 }],
			[{ "k": 1401 }],
			[],
			{}
		],
		[
			1,
			"transip-nl",
			2,
			"^https://www\\.transip\\.nl/",
			22,
			[1402],
			[{ "any": [{ "e": 1402 }, { "e": 1403 }] }],
			[{ "any": [{ "v": 1402 }, { "v": 1403 }] }],
			[{
				"if": { "e": 1403 },
				"then": [{ "k": 1404 }],
				"else": [{ "k": 1405 }]
			}],
			[],
			{}
		],
		[
			1,
			"truecar",
			1,
			"^https://(www\\.)?truecar\\.com/",
			22,
			[1406],
			[{ "e": 1407 }],
			[{ "v": 1407 }],
			[{ "h": 1406 }],
			[],
			{}
		],
		[
			1,
			"twitch.tv",
			2,
			"^https?://(www\\.)?twitch\\.tv",
			22,
			[1408],
			[{ "e": 1409 }],
			[{ "v": 1409 }],
			[
				{ "h": 1410 },
				{ "k": 1411 },
				{ "w": 1412 },
				{
					"all": true,
					"optional": true,
					"k": 1413
				},
				{ "c": 1414 },
				{
					"check": "none",
					"wv": 1415
				}
			],
			[],
			{}
		],
		[
			1,
			"twitter",
			2,
			"^https://([a-z0-9-]+\\.)?(twitter|x)\\.com/",
			22,
			[1416],
			[{ "e": 1417 }],
			[{ "v": 1417 }],
			[{ "c": 1417 }],
			[{
				"timeout": 1e3,
				"check": "none",
				"wv": 1417
			}],
			{}
		],
		[
			1,
			"unicourt",
			1,
			"^https://(www\\.)?unicourt\\.com/",
			22,
			[1418],
			[{ "e": 1418 }],
			[{ "v": 1418 }],
			[{ "h": 1418 }],
			[],
			{}
		],
		[
			1,
			"uswitch.com",
			2,
			"^https://(www\\.)?uswitch\\.com/",
			10,
			[1419],
			[{ "e": 1420 }],
			[{ "v": 1420 }],
			[{ "c": 1421 }],
			[],
			{}
		],
		[
			1,
			"vodafone.de",
			2,
			"^https://www\\.vodafone\\.de/",
			22,
			[1422],
			[{ "e": 1423 }],
			[{ "v": 1424 }],
			[{ "k": 1425 }],
			[],
			{}
		],
		[
			1,
			"volvocars-com",
			2,
			"^https://([a-z0-9-]+\\.)?volvocars\\.com/",
			10,
			[1426],
			[{ "exists": ["cookie-banner#cookie-banner-host", "#dialog_cookie_consent"] }],
			[{ "visible": ["cookie-banner#cookie-banner-host", "#dialog_cookie_consent"] }],
			[
				{ "wait": 500 },
				{ "waitForThenClick": ["cookie-banner#cookie-banner-host", "#onetrust-reject-all-handler"] },
				{
					"waitForVisible": ["cookie-banner#cookie-banner-host", "#dialog_cookie_consent"],
					"check": "none",
					"timeout": 5e3
				}
			],
			[{ "cc": 1427 }],
			{}
		],
		[
			1,
			"wikiwand",
			1,
			"^https://(www\\.)?wikiwand\\.com/",
			22,
			[1428],
			[{ "e": 1428 }],
			[{ "v": 1428 }],
			[{ "h": 1428 }],
			[],
			{}
		],
		[
			1,
			"xe.com",
			2,
			"^https://www\\.xe\\.com/",
			22,
			[1429],
			[{ "e": 1429 }],
			[{ "v": 1429 }],
			[
				{ "wait": 1e3 },
				{ "c": 1430 },
				{ "c": 1431 }
			],
			[{ "cc": 1432 }],
			{}
		],
		[
			1,
			"xhamster-eu",
			2,
			"^https://(\\w+\\.)?xhamster\\d?\\.com",
			22,
			[1433],
			[{ "e": 1434 }],
			[{ "v": 1434 }],
			[{ "c": 1435 }],
			[],
			{}
		],
		[
			1,
			"xhamster-us",
			1,
			"^https://(\\w+\\.)?xhamster\\d?\\.com",
			22,
			[1436],
			[{ "e": 1436 }],
			[{ "v": 1437 }],
			[{ "h": 1436 }],
			[],
			{}
		],
		[
			1,
			"xvideos",
			2,
			"^https://[a-z]*\\.xvideos\\.com/",
			22,
			[],
			[{ "e": 1438 }],
			[{ "v": 1438 }],
			[{ "c": 1439 }],
			[],
			{}
		],
		[
			1,
			"Yahoo",
			2,
			"^https://consent\\.yahoo\\.com/v2/",
			22,
			[615],
			[{ "e": 1440 }],
			[{ "v": 1440 }],
			[{ "c": 1441 }],
			[],
			{}
		],
		[
			1,
			"zentralruf-de",
			2,
			"^https://(www\\.)?zentralruf\\.de",
			22,
			[1442],
			[{ "e": 1442 }],
			[{ "v": 1442 }],
			[{ "c": 1443 }],
			[],
			{}
		],
		[
			1,
			"zinio",
			0,
			"^https://(www\\.)?zinio\\.com/",
			22,
			[],
			[{ "e": 1444 }],
			[{ "v": 1444 }],
			[{ "c": 1445 }, { "c": 1446 }],
			[{ "cc": 1447 }],
			{}
		]
	],
	index: {
		"genericRuleRange": [0, 185],
		"frameRuleRange": [184, 210],
		"specificRuleRange": [185, 760],
		"genericStringEnd": 722,
		"frameStringEnd": 757
	}
};
//#endregion
export { compact_rules_default as default };
