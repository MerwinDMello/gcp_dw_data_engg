CREATE OR REPLACE VIEW edwpsc_views.`epic_factclaimunbilledstatus`
AS SELECT
  `epic_factclaimunbilledstatus`.claimkey,
  `epic_factclaimunbilledstatus`.claimnumber,
  `epic_factclaimunbilledstatus`.regionkey,
  `epic_factclaimunbilledstatus`.coid,
  `epic_factclaimunbilledstatus`.patientinternalid,
  `epic_factclaimunbilledstatus`.visitnumber,
  `epic_factclaimunbilledstatus`.unbilledstatuskey,
  `epic_factclaimunbilledstatus`.claimunbilledstatusinrhinventory,
  `epic_factclaimunbilledstatus`.claimunbilledstatusrhholdcode,
  `epic_factclaimunbilledstatus`.claimunbilledstatusedinohold,
  `epic_factclaimunbilledstatus`.claimunbilledstatusminsubmissiondate,
  `epic_factclaimunbilledstatus`.claimunbilledstatusmaxsubmissiondate,
  `epic_factclaimunbilledstatus`.claimunbilledstatusclaimstatus,
  `epic_factclaimunbilledstatus`.inserteddtm,
  `epic_factclaimunbilledstatus`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`epic_factclaimunbilledstatus`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factclaimunbilledstatus`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;