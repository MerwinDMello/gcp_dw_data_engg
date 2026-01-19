CREATE OR REPLACE VIEW edwpsc_base_views.`pv_refpaymentbatch`
AS SELECT
  `pv_refpaymentbatch`.batchkey,
  `pv_refpaymentbatch`.batchname,
  `pv_refpaymentbatch`.batchdatekey,
  `pv_refpaymentbatch`.sourceprimarykeyvalue,
  `pv_refpaymentbatch`.sourcerecordlastupdated,
  `pv_refpaymentbatch`.dwlastupdatedatetime,
  `pv_refpaymentbatch`.sourcesystemcode,
  `pv_refpaymentbatch`.insertedby,
  `pv_refpaymentbatch`.inserteddtm,
  `pv_refpaymentbatch`.modifiedby,
  `pv_refpaymentbatch`.modifieddtm,
  `pv_refpaymentbatch`.deleteflag,
  `pv_refpaymentbatch`.regionkey
  FROM
    edwpsc.`pv_refpaymentbatch`
;