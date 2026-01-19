CREATE OR REPLACE VIEW edwpsc_base_views.`epic_refpaymentbatch`
AS SELECT
  `epic_refpaymentbatch`.batchkey,
  `epic_refpaymentbatch`.batchname,
  `epic_refpaymentbatch`.batchdatekey,
  `epic_refpaymentbatch`.batchopendate,
  `epic_refpaymentbatch`.batchcloseddate,
  `epic_refpaymentbatch`.batchtotalamt,
  `epic_refpaymentbatch`.batchpostedamt,
  `epic_refpaymentbatch`.batchstatus,
  `epic_refpaymentbatch`.remitflag,
  `epic_refpaymentbatch`.batchtypedesc,
  `epic_refpaymentbatch`.batchnote,
  `epic_refpaymentbatch`.createdbyuserkey,
  `epic_refpaymentbatch`.closedbyuserkey,
  `epic_refpaymentbatch`.regionkey,
  `epic_refpaymentbatch`.deleteflag,
  `epic_refpaymentbatch`.sourceaprimarykeyvalue,
  `epic_refpaymentbatch`.sourcearecordlastupdated,
  `epic_refpaymentbatch`.dwlastupdatedatetime,
  `epic_refpaymentbatch`.sourcesystemcode,
  `epic_refpaymentbatch`.insertedby,
  `epic_refpaymentbatch`.inserteddtm,
  `epic_refpaymentbatch`.modifiedby,
  `epic_refpaymentbatch`.modifieddtm
  FROM
    edwpsc.`epic_refpaymentbatch`
;