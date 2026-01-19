CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspeppi_pspeppinotes
(
  note_cnt NUMERIC(29) NOT NULL,
  note_date DATETIME,
  note_time DATETIME,
  note_type STRING,
  note_user STRING,
  pspeppikey STRING NOT NULL,
  pspeppinotes STRING,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (pspeppikey, note_cnt) NOT ENFORCED
)
;