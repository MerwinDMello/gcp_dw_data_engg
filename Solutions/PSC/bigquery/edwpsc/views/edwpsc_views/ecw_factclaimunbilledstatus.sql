CREATE OR REPLACE VIEW edwpsc_views.`ecw_factclaimunbilledstatus`
AS SELECT
  `ecw_factclaimunbilledstatus`.claimkey,
  `ecw_factclaimunbilledstatus`.claimnumber,
  `ecw_factclaimunbilledstatus`.unbilledstatuskey,
  `ecw_factclaimunbilledstatus`.claimunbilledstatusinrhinventory,
  `ecw_factclaimunbilledstatus`.claimunbilledstatusrhholdcode,
  `ecw_factclaimunbilledstatus`.claimunbilledstatusedinohold,
  `ecw_factclaimunbilledstatus`.claimunbilledstatusminsubmissiondate,
  `ecw_factclaimunbilledstatus`.claimunbilledstatusmaxsubmissiondate,
  `ecw_factclaimunbilledstatus`.claimunbilledstatusclaimstatus,
  `ecw_factclaimunbilledstatus`.coid,
  `ecw_factclaimunbilledstatus`.regionkey,
  `ecw_factclaimunbilledstatus`.inserteddtm,
  `ecw_factclaimunbilledstatus`.rhunbilledcategory,
  `ecw_factclaimunbilledstatus`.holdcategory,
  `ecw_factclaimunbilledstatus`.claimstatusowner
  FROM
    edwpsc_base_views.`ecw_factclaimunbilledstatus`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factclaimunbilledstatus`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;