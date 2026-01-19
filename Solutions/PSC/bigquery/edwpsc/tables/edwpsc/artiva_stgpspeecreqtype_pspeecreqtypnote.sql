CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspeecreqtype_pspeecreqtypnote
(
  note_cnt NUMERIC(29) NOT NULL,
  note_date DATETIME,
  note_time DATETIME,
  note_type STRING,
  note_user STRING,
  pspeecreqtypnote STRING,
  pspeecrtid STRING NOT NULL,
  PRIMARY KEY (pspeecrtid, note_cnt) NOT ENFORCED
)
;