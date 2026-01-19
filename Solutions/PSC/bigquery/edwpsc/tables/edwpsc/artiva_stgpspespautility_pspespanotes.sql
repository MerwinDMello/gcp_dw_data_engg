CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspespautility_pspespanotes
(
  note_cnt INT64 NOT NULL,
  note_date DATE,
  note_time TIME,
  notedatetime DATETIME,
  note_type STRING,
  note_user STRING,
  pspespakey STRING NOT NULL,
  pspespanotes STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (pspespakey, note_cnt) NOT ENFORCED
)
;