CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspecpids_pspecpidnotes
(
  note_cnt NUMERIC(29) NOT NULL,
  note_date DATETIME,
  note_time DATETIME,
  note_type STRING,
  note_user STRING,
  pspecpidkey STRING NOT NULL,
  pspecpidnotes STRING,
  PRIMARY KEY (pspecpidkey, note_cnt) NOT ENFORCED
)
;