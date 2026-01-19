CREATE OR REPLACE VIEW edwpsc_views.`artiva_stghcaccount_hcacnotes`
AS SELECT
  `artiva_stghcaccount_hcacnotes`.hcacid,
  `artiva_stghcaccount_hcacnotes`.hcacnotes,
  `artiva_stghcaccount_hcacnotes`.note_cnt,
  `artiva_stghcaccount_hcacnotes`.note_date,
  `artiva_stghcaccount_hcacnotes`.note_time,
  `artiva_stghcaccount_hcacnotes`.notedatetime,
  `artiva_stghcaccount_hcacnotes`.note_type,
  `artiva_stghcaccount_hcacnotes`.note_user,
  `artiva_stghcaccount_hcacnotes`.hcpatientaccountingnumber,
  `artiva_stghcaccount_hcacnotes`.ecwclaimkey,
  `artiva_stghcaccount_hcacnotes`.ecwclaimnumber,
  `artiva_stghcaccount_hcacnotes`.ecwregionkey,
  `artiva_stghcaccount_hcacnotes`.pvclaimkey,
  `artiva_stghcaccount_hcacnotes`.pvclaimnumber,
  `artiva_stghcaccount_hcacnotes`.pvregionkey,
  `artiva_stghcaccount_hcacnotes`.insertedby,
  `artiva_stghcaccount_hcacnotes`.inserteddtm,
  `artiva_stghcaccount_hcacnotes`.modifiedby,
  `artiva_stghcaccount_hcacnotes`.modifieddtm,
  `artiva_stghcaccount_hcacnotes`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`artiva_stghcaccount_hcacnotes`
;