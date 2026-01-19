CREATE OR REPLACE VIEW edwpsc_views.`pv_factsnapshotchangesinunits`
AS SELECT
  `pv_factsnapshotchangesinunits`.unitskey,
  `pv_factsnapshotchangesinunits`.monthid,
  `pv_factsnapshotchangesinunits`.snapshotdate,
  `pv_factsnapshotchangesinunits`.coid,
  `pv_factsnapshotchangesinunits`.regionkey,
  `pv_factsnapshotchangesinunits`.practiceid,
  `pv_factsnapshotchangesinunits`.cptid,
  `pv_factsnapshotchangesinunits`.differenceunitsvalue,
  `pv_factsnapshotchangesinunits`.differencechargesvalue,
  `pv_factsnapshotchangesinunits`.foundinpreviousmonth,
  `pv_factsnapshotchangesinunits`.deletedline,
  `pv_factsnapshotchangesinunits`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pv_factsnapshotchangesinunits`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factsnapshotchangesinunits`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;