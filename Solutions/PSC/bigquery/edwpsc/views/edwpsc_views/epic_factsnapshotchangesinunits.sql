CREATE OR REPLACE VIEW edwpsc_views.`epic_factsnapshotchangesinunits`
AS SELECT
  `epic_factsnapshotchangesinunits`.unitskey,
  `epic_factsnapshotchangesinunits`.monthid,
  `epic_factsnapshotchangesinunits`.snapshotdate,
  `epic_factsnapshotchangesinunits`.coid,
  `epic_factsnapshotchangesinunits`.regionkey,
  `epic_factsnapshotchangesinunits`.practiceid,
  `epic_factsnapshotchangesinunits`.cptid,
  `epic_factsnapshotchangesinunits`.claimkey,
  `epic_factsnapshotchangesinunits`.differenceunitsvalue,
  `epic_factsnapshotchangesinunits`.differencechargesvalue,
  `epic_factsnapshotchangesinunits`.foundinpreviousmonth,
  `epic_factsnapshotchangesinunits`.deletedline,
  `epic_factsnapshotchangesinunits`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`epic_factsnapshotchangesinunits`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factsnapshotchangesinunits`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;