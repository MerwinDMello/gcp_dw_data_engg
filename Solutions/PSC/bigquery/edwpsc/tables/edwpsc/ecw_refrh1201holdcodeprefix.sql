CREATE TABLE IF NOT EXISTS edwpsc.ecw_refrh1201holdcodeprefix
(
  rh1201holdcodeprefixkey STRING NOT NULL,
  rh1201holdcodeprefixname STRING NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64,
  PRIMARY KEY (rh1201holdcodeprefixkey) NOT ENFORCED
)
;