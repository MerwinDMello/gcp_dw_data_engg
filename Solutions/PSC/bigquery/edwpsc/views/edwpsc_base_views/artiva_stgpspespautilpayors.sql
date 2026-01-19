CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspespautilpayors`
AS SELECT
  `artiva_stgpspespautilpayors`.pspespatmppayid,
  `artiva_stgpspespautilpayors`.pspespatmppaykey,
  `artiva_stgpspespautilpayors`.pspespatmppayprocflg,
  `artiva_stgpspespautilpayors`.pspespatmppayspaid,
  `artiva_stgpspespautilpayors`.pspespatmppayovrflg
  FROM
    edwpsc.`artiva_stgpspespautilpayors`
;