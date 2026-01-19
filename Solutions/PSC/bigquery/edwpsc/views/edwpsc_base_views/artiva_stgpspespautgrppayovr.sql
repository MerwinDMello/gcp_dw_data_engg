CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspespautgrppayovr`
AS SELECT
  `artiva_stgpspespautgrppayovr`.pspespautgpovkey,
  `artiva_stgpspespautgrppayovr`.pspespautgpovpayid,
  `artiva_stgpspespautgrppayovr`.pspespautgpovspaid
  FROM
    edwpsc.`artiva_stgpspespautgrppayovr`
;