CREATE OR REPLACE VIEW edwpsc_views.`pv_factencountercpt`
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
    edwpsc_base_views.`pv_factencountercpt`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factencountercpt`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;