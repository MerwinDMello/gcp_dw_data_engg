CREATE OR REPLACE VIEW edwpsc_views.`epic_junccontractualadjustmentmatchingtransactions`
AS SELECT
  `epic_junccontractualadjustmentmatchingtransactions`.transactionid,
  `epic_junccontractualadjustmentmatchingtransactions`.matchingtransactionid,
  `epic_junccontractualadjustmentmatchingtransactions`.regionkey,
  `epic_junccontractualadjustmentmatchingtransactions`.insertedby,
  `epic_junccontractualadjustmentmatchingtransactions`.inserteddtm,
  `epic_junccontractualadjustmentmatchingtransactions`.modifiedby,
  `epic_junccontractualadjustmentmatchingtransactions`.modifieddtm,
  `epic_junccontractualadjustmentmatchingtransactions`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`epic_junccontractualadjustmentmatchingtransactions`
;