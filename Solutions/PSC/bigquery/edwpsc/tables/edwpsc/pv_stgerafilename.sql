CREATE TABLE IF NOT EXISTS edwpsc.pv_stgerafilename
(
  fileid INT64 NOT NULL,
  filename STRING NOT NULL,
  fullfilename STRING NOT NULL,
  filedate DATE,
  isa06 STRING,
  isa07 STRING,
  isa08 STRING,
  isa09 STRING,
  isa10 STRING,
  isa11 STRING,
  isa12 STRING,
  isa13 STRING,
  isa14 STRING,
  isa15 STRING,
  isa16 STRING,
  isasegment STRING,
  inserteddtm DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL
)
;