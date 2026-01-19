CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspespautgrppayovr`
AS SELECT
  `artiva_stgpspespautgrppayovr`.pspespautgpovkey,
  `artiva_stgpspespautgrppayovr`.pspespautgpovpayid,
  `artiva_stgpspespautgrppayovr`.pspespautgpovspaid
  FROM
    edwpsc_base_views.`artiva_stgpspespautgrppayovr`
;