CREATE OR REPLACE VIEW edwpsc_base_views.`pv_factclaimunbilled`
AS SELECT
  `pv_factclaimunbilled`.regionkey,
  `pv_factclaimunbilled`.coid,
  `pv_factclaimunbilled`.practice,
  `pv_factclaimunbilled`.claimkey,
  `pv_factclaimunbilled`.claimnumber,
  `pv_factclaimunbilled`.unbilledstatuskey,
  `pv_factclaimunbilled`.claimunbilledstatusinrhinventory,
  `pv_factclaimunbilled`.claimunbilledstatusrhholdcode,
  `pv_factclaimunbilled`.claimunbilledstatusedinohold,
  `pv_factclaimunbilled`.claimunbilledstatusminsubmissiondate,
  `pv_factclaimunbilled`.claimunbilledstatusmaxsubmissiondate,
  `pv_factclaimunbilled`.claimunbilledstatusclaimstatus,
  `pv_factclaimunbilled`.insertedby,
  `pv_factclaimunbilled`.inserteddtm,
  `pv_factclaimunbilled`.rhunbilledcategory,
  `pv_factclaimunbilled`.claimstatusowner,
  `pv_factclaimunbilled`.dwlastupdatedatetime
  FROM
    edwpsc.`pv_factclaimunbilled`
;