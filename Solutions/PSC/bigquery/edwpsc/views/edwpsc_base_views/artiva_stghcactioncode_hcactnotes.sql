CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stghcactioncode_hcactnotes`
AS SELECT
  `artiva_stghcactioncode_hcactnotes`.hcactid,
  `artiva_stghcactioncode_hcactnotes`.hcactnotes,
  `artiva_stghcactioncode_hcactnotes`.note_cnt,
  `artiva_stghcactioncode_hcactnotes`.note_date,
  `artiva_stghcactioncode_hcactnotes`.note_time,
  `artiva_stghcactioncode_hcactnotes`.notedatetime,
  `artiva_stghcactioncode_hcactnotes`.note_type,
  `artiva_stghcactioncode_hcactnotes`.note_user,
  `artiva_stghcactioncode_hcactnotes`.insertedby,
  `artiva_stghcactioncode_hcactnotes`.inserteddtm,
  `artiva_stghcactioncode_hcactnotes`.modifiedby,
  `artiva_stghcactioncode_hcactnotes`.modifieddtm,
  `artiva_stghcactioncode_hcactnotes`.dwlastupdatedatetime
  FROM
    edwpsc.`artiva_stghcactioncode_hcactnotes`
;