CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspepayer_pspepaynotes
(
  note_cnt NUMERIC(29) NOT NULL,
  note_date DATETIME,
  note_time DATETIME,
  note_type STRING,
  note_user STRING,
  pspepaykey STRING NOT NULL,
  pspepaynotes STRING,
  PRIMARY KEY (pspepaykey, note_cnt) NOT ENFORCED
)
;