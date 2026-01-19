CREATE OR REPLACE VIEW edwpsc_base_views.`epic_juncpaymentmatchingtransactions`
AS SELECT
  `epic_juncpaymentmatchingtransactions`.transactionid,
  `epic_juncpaymentmatchingtransactions`.matchingtransactionid,
  `epic_juncpaymentmatchingtransactions`.regionkey,
  `epic_juncpaymentmatchingtransactions`.insertedby,
  `epic_juncpaymentmatchingtransactions`.inserteddtm,
  `epic_juncpaymentmatchingtransactions`.modifiedby,
  `epic_juncpaymentmatchingtransactions`.modifieddtm,
  `epic_juncpaymentmatchingtransactions`.dwlastupdatedatetime
  FROM
    edwpsc.`epic_juncpaymentmatchingtransactions`
;