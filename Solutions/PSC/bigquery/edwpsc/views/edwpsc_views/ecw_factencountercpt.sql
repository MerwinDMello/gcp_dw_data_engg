CREATE OR REPLACE VIEW edwpsc_views.`ecw_factencountercpt`
AS SELECT
  `ecw_factencountercpt`.encountercptkey,
  `ecw_factencountercpt`.regionkey,
  `ecw_factencountercpt`.coid,
  `ecw_factencountercpt`.encounterkey,
  `ecw_factencountercpt`.encounterid,
  `ecw_factencountercpt`.cptcodekey,
  `ecw_factencountercpt`.cptcode,
  `ecw_factencountercpt`.cptunits,
  `ecw_factencountercpt`.cptorder,
  `ecw_factencountercpt`.visitdate,
  `ecw_factencountercpt`.primaryflag,
  `ecw_factencountercpt`.deleteflag,
  `ecw_factencountercpt`.sourceprimarykeyvalue,
  `ecw_factencountercpt`.sourcerecordlastupdated,
  `ecw_factencountercpt`.dwlastupdatedatetime,
  `ecw_factencountercpt`.sourcesystemcode,
  `ecw_factencountercpt`.insertedby,
  `ecw_factencountercpt`.inserteddtm,
  `ecw_factencountercpt`.modifiedby,
  `ecw_factencountercpt`.modifieddtm,
  `ecw_factencountercpt`.archivedrecord
  FROM
    edwpsc_base_views.`ecw_factencountercpt`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factencountercpt`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;