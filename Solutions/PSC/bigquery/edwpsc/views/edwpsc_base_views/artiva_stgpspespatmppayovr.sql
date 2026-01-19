CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspespatmppayovr`
AS SELECT
  `artiva_stgpspespatmppayovr`.pspespayovrkey,
  `artiva_stgpspespatmppayovr`.pspespayovrlocflg,
  `artiva_stgpspespatmppayovr`.pspespayovrperfid,
  `artiva_stgpspespatmppayovr`.pspespayovrpid
  FROM
    edwpsc.`artiva_stgpspespatmppayovr`
;