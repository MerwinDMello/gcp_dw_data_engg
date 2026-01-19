CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpsccupicklistmaint`
AS SELECT
  `artiva_stgpsccupicklistmaint`.psccupklstkey,
  `artiva_stgpsccupicklistmaint`.psccupklstactive,
  `artiva_stgpsccupicklistmaint`.psccupklstfieldnm,
  `artiva_stgpsccupicklistmaint`.psccupklstfldvalue,
  `artiva_stgpsccupicklistmaint`.sourcesystemcode,
  `artiva_stgpsccupicklistmaint`.dwlastupdatedatetime,
  `artiva_stgpsccupicklistmaint`.insertedby,
  `artiva_stgpsccupicklistmaint`.inserteddtm,
  `artiva_stgpsccupicklistmaint`.modifiedby,
  `artiva_stgpsccupicklistmaint`.modifieddtm,
  `artiva_stgpsccupicklistmaint`.psccupklstactdte,
  `artiva_stgpsccupicklistmaint`.psccupklstinactdate,
  `artiva_stgpsccupicklistmaint`.psccupklstlastupdusr,
  `artiva_stgpsccupicklistmaint`.psccupklstupddte
  FROM
    edwpsc.`artiva_stgpsccupicklistmaint`
;