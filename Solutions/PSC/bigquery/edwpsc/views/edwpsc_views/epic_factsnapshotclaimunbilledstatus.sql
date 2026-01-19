CREATE OR REPLACE VIEW edwpsc_views.`epic_factsnapshotclaimunbilledstatus`
AS SELECT
  `epic_factsnapshotclaimunbilledstatus`.unbilledstatussnapshotkey,
  `epic_factsnapshotclaimunbilledstatus`.monthid,
  `epic_factsnapshotclaimunbilledstatus`.snapshotdate,
  `epic_factsnapshotclaimunbilledstatus`.claimkey,
  `epic_factsnapshotclaimunbilledstatus`.claimnumber,
  `epic_factsnapshotclaimunbilledstatus`.regionkey,
  `epic_factsnapshotclaimunbilledstatus`.coid,
  `epic_factsnapshotclaimunbilledstatus`.patientinternalid,
  `epic_factsnapshotclaimunbilledstatus`.visitnumber,
  `epic_factsnapshotclaimunbilledstatus`.unbilledstatuskey,
  `epic_factsnapshotclaimunbilledstatus`.claimunbilledstatusinrhinventory,
  `epic_factsnapshotclaimunbilledstatus`.claimunbilledstatusrhholdcode,
  `epic_factsnapshotclaimunbilledstatus`.claimunbilledstatusedinohold,
  `epic_factsnapshotclaimunbilledstatus`.claimunbilledstatusminsubmissiondate,
  `epic_factsnapshotclaimunbilledstatus`.claimunbilledstatusmaxsubmissiondate,
  `epic_factsnapshotclaimunbilledstatus`.claimunbilledstatusclaimstatus,
  `epic_factsnapshotclaimunbilledstatus`.insertedby,
  `epic_factsnapshotclaimunbilledstatus`.inserteddtm
  FROM
    edwpsc_base_views.`epic_factsnapshotclaimunbilledstatus`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factsnapshotclaimunbilledstatus`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;