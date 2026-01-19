CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpenoteshistory
(
  penoteshistorykey INT64 NOT NULL,
  pspeppikey STRING,
  notecount INT64,
  notedate DATE,
  notetime TIME,
  notedatetime DATETIME,
  notetype STRING,
  notecreatedbyuserid STRING,
  note STRING,
  notesource STRING,
  sourceaprimarykeyvalue STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;