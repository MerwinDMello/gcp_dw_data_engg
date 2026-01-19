CREATE OR REPLACE VIEW edwpsc_base_views.`epic_factsnapshotclaimunbilledstatus`
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
    edwpsc.`epic_factsnapshotclaimunbilledstatus`
;