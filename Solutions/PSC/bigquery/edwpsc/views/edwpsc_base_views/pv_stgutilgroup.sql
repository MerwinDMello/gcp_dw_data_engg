CREATE OR REPLACE VIEW edwpsc_base_views.`pv_stgutilgroup`
AS SELECT
  `pv_stgutilgroup`.group_name,
  `pv_stgutilgroup`.entity,
  `pv_stgutilgroup`.key_name,
  `pv_stgutilgroup`.subkey,
  `pv_stgutilgroup`.active,
  `pv_stgutilgroup`.text,
  `pv_stgutilgroup`.number,
  `pv_stgutilgroup`.description,
  `pv_stgutilgroup`.utilgrouppk,
  `pv_stgutilgroup`.isupdateable,
  `pv_stgutilgroup`.inserteddtm,
  `pv_stgutilgroup`.modifieddtm,
  `pv_stgutilgroup`.dwlastupdatedatetime,
  `pv_stgutilgroup`.sourcephysicaldeleteflag
  FROM
    edwpsc.`pv_stgutilgroup`
;