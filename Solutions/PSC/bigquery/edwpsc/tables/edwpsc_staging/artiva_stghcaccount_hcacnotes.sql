CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stghcaccount_hcacnotes
(
  hcacid INT64,
  hcacnotes STRING,
  note_cnt INT64,
  note_date DATE,
  note_time TIME,
  notedatetime DATETIME,
  note_type STRING,
  note_user STRING,
  hcpatientaccountingnumber STRING,
  ecwclaimkey INT64,
  ecwclaimnumber INT64,
  ecwregionkey INT64,
  pvclaimkey INT64,
  pvclaimnumber INT64,
  pvregionkey INT64,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;