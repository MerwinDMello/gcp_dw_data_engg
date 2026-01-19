CREATE TABLE IF NOT EXISTS edwpsc.ecw_refbilltype
(
  billtypekey INT64 NOT NULL,
  billtypename STRING NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (billtypekey) NOT ENFORCED
)
;