CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refpaymentbatch`
AS SELECT
  `ecw_refpaymentbatch`.batchkey,
  `ecw_refpaymentbatch`.batchname,
  `ecw_refpaymentbatch`.batchdatekey,
  `ecw_refpaymentbatch`.sourceprimarykeyvalue,
  `ecw_refpaymentbatch`.sourcerecordlastupdated,
  `ecw_refpaymentbatch`.dwlastupdatedatetime,
  `ecw_refpaymentbatch`.sourcesystemcode,
  `ecw_refpaymentbatch`.insertedby,
  `ecw_refpaymentbatch`.inserteddtm,
  `ecw_refpaymentbatch`.modifiedby,
  `ecw_refpaymentbatch`.modifieddtm,
  `ecw_refpaymentbatch`.deleteflag,
  `ecw_refpaymentbatch`.regionkey
  FROM
    edwpsc.`ecw_refpaymentbatch`
;