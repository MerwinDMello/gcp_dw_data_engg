CREATE OR REPLACE VIEW edwpsc_views.`cmt_factmonthlytransactions`
AS SELECT
  `cmt_factmonthlytransactions`.tbnumber,
  `cmt_factmonthlytransactions`.transactiontype,
  `cmt_factmonthlytransactions`.posttype,
  `cmt_factmonthlytransactions`.posteddate,
  `cmt_factmonthlytransactions`.amountposted,
  `cmt_factmonthlytransactions`.user34id,
  `cmt_factmonthlytransactions`.firstname,
  `cmt_factmonthlytransactions`.lastname,
  `cmt_factmonthlytransactions`.insertedby,
  `cmt_factmonthlytransactions`.inserteddtm,
  `cmt_factmonthlytransactions`.modifiedby,
  `cmt_factmonthlytransactions`.modifieddtm,
  `cmt_factmonthlytransactions`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`cmt_factmonthlytransactions`
;