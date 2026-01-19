CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspesysalerts_pspesysalrnotes
(
  note_cnt NUMERIC(29) NOT NULL,
  note_date DATETIME,
  note_time DATETIME,
  note_type STRING,
  note_user STRING,
  pspesysalrkey STRING NOT NULL,
  pspesysalrnotes STRING,
  PRIMARY KEY (pspesysalrkey, note_cnt) NOT ENFORCED
)
;