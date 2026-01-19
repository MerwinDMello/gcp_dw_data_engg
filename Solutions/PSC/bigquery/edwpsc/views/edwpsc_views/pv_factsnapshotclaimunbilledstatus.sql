CREATE OR REPLACE VIEW edwpsc_views.`pv_factsnapshotclaimunbilledstatus`
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
    edwpsc_base_views.`pv_factsnapshotclaimunbilledstatus`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factsnapshotclaimunbilledstatus`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;