CREATE OR REPLACE VIEW edwpsc_base_views.`cmt_facttreasurybatch`
AS SELECT
  `cmt_facttreasurybatch`.treasurybatchkey,
  `cmt_facttreasurybatch`.paymentid,
  `cmt_facttreasurybatch`.regionid,
  `cmt_facttreasurybatch`.sourcesystem,
  `cmt_facttreasurybatch`.batchid,
  `cmt_facttreasurybatch`.createdby,
  `cmt_facttreasurybatch`.batchcreatedate,
  `cmt_facttreasurybatch`.insertedby,
  `cmt_facttreasurybatch`.inserteddtm,
  `cmt_facttreasurybatch`.modifiedby,
  `cmt_facttreasurybatch`.modifieddtm,
  `cmt_facttreasurybatch`.dwlastupdatedatetime
  FROM
    edwpsc.`cmt_facttreasurybatch`
;