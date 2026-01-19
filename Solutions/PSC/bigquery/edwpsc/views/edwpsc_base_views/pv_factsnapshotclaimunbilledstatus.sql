CREATE OR REPLACE VIEW edwpsc_base_views.`pv_factsnapshotclaimunbilledstatus`
AS SELECT
  `pv_factsnapshotclaimunbilledstatus`.unbilledstatussnapshotkey,
  `pv_factsnapshotclaimunbilledstatus`.regionkey,
  `pv_factsnapshotclaimunbilledstatus`.coid,
  `pv_factsnapshotclaimunbilledstatus`.practice,
  `pv_factsnapshotclaimunbilledstatus`.claimkey,
  `pv_factsnapshotclaimunbilledstatus`.claimnumber,
  `pv_factsnapshotclaimunbilledstatus`.unbilledstatuskey,
  `pv_factsnapshotclaimunbilledstatus`.claimunbilledstatusinrhinventory,
  `pv_factsnapshotclaimunbilledstatus`.claimunbilledstatusrhholdcode,
  `pv_factsnapshotclaimunbilledstatus`.claimunbilledstatusedinohold,
  `pv_factsnapshotclaimunbilledstatus`.claimunbilledstatusminsubmissiondate,
  `pv_factsnapshotclaimunbilledstatus`.claimunbilledstatusmaxsubmissiondate,
  `pv_factsnapshotclaimunbilledstatus`.claimunbilledstatusclaimstatus,
  `pv_factsnapshotclaimunbilledstatus`.insertedby,
  `pv_factsnapshotclaimunbilledstatus`.inserteddtm,
  `pv_factsnapshotclaimunbilledstatus`.modifiedby,
  `pv_factsnapshotclaimunbilledstatus`.modifieddtm,
  `pv_factsnapshotclaimunbilledstatus`.snapshotdate,
  `pv_factsnapshotclaimunbilledstatus`.rhunbilledcategory,
  `pv_factsnapshotclaimunbilledstatus`.claimstatusowner,
  `pv_factsnapshotclaimunbilledstatus`.dwlastupdatedatetime
  FROM
    edwpsc.`pv_factsnapshotclaimunbilledstatus`
;