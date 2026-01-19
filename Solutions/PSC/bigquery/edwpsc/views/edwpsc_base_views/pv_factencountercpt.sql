CREATE OR REPLACE VIEW edwpsc_base_views.`pv_factencountercpt`
AS SELECT
  `pv_factencountercpt`.encountercptkey,
  `pv_factencountercpt`.regionkey,
  `pv_factencountercpt`.coid,
  `pv_factencountercpt`.practicename,
  `pv_factencountercpt`.encounterkey,
  `pv_factencountercpt`.encounterid,
  `pv_factencountercpt`.cptcodekey,
  `pv_factencountercpt`.cptcode,
  `pv_factencountercpt`.cptunits,
  `pv_factencountercpt`.visitdate,
  `pv_factencountercpt`.primaryflag,
  `pv_factencountercpt`.deleteflag,
  `pv_factencountercpt`.sourceprimarykeyvalue,
  `pv_factencountercpt`.sourcerecordlastupdated,
  `pv_factencountercpt`.dwlastupdatedatetime,
  `pv_factencountercpt`.sourcesystemcode,
  `pv_factencountercpt`.insertedby,
  `pv_factencountercpt`.inserteddtm,
  `pv_factencountercpt`.modifiedby,
  `pv_factencountercpt`.modifieddtm
  FROM
    edwpsc.`pv_factencountercpt`
;