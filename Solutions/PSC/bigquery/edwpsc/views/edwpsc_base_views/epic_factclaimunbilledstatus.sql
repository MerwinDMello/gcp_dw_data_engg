CREATE OR REPLACE VIEW edwpsc_base_views.`epic_factclaimunbilledstatus`
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
    edwpsc.`epic_factclaimunbilledstatus`
;