CREATE TABLE IF NOT EXISTS edwpsc.ecw_refglstatcode
(
  glstatcodekey STRING NOT NULL,
  statcodedesc STRING,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (glstatcodekey) NOT ENFORCED
)
;