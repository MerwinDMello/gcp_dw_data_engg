CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpsholdrulesutility_pshrunotes
(
  note_cnt NUMERIC(29) NOT NULL,
  note_date DATETIME,
  note_time TIME,
  note_type STRING,
  note_user STRING,
  pshrukey NUMERIC(29) NOT NULL,
  pshrunotes STRING,
  notedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (pshrukey, note_cnt) NOT ENFORCED
)
;