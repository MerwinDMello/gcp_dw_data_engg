CREATE TABLE IF NOT EXISTS edwpsc.eboc_facttransactions
(
  keyid STRING NOT NULL,
  username STRING,
  userid STRING,
  gldate STRING,
  linecount STRING,
  dollars NUMERIC(33, 4),
  treasurybatchnumber STRING,
  dwlastupdatedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;