CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_factsnapshotclaimunbilledstatus`
AS SELECT
  `ecw_factsnapshotclaimunbilledstatus`.claimkey,
  `ecw_factsnapshotclaimunbilledstatus`.monthid,
  `ecw_factsnapshotclaimunbilledstatus`.snapshotdate,
  `ecw_factsnapshotclaimunbilledstatus`.claimnumber,
  `ecw_factsnapshotclaimunbilledstatus`.unbilledstatuskey,
  `ecw_factsnapshotclaimunbilledstatus`.claimunbilledstatusinrhinventory,
  `ecw_factsnapshotclaimunbilledstatus`.claimunbilledstatusrhholdcode,
  `ecw_factsnapshotclaimunbilledstatus`.claimunbilledstatusedinohold,
  `ecw_factsnapshotclaimunbilledstatus`.claimunbilledstatusminsubmissiondate,
  `ecw_factsnapshotclaimunbilledstatus`.claimunbilledstatusmaxsubmissiondate,
  `ecw_factsnapshotclaimunbilledstatus`.claimunbilledstatusclaimstatus,
  `ecw_factsnapshotclaimunbilledstatus`.coid,
  `ecw_factsnapshotclaimunbilledstatus`.rhunbilledcategory,
  `ecw_factsnapshotclaimunbilledstatus`.holdcategory,
  `ecw_factsnapshotclaimunbilledstatus`.claimstatusowner
  FROM
    edwpsc.`ecw_factsnapshotclaimunbilledstatus`
;