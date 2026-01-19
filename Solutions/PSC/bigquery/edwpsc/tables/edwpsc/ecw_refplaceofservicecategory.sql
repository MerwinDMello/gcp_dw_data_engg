CREATE TABLE IF NOT EXISTS edwpsc.ecw_refplaceofservicecategory
(
  poscategorykey INT64 NOT NULL,
  poscategoryname STRING NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (poscategorykey) NOT ENFORCED
)
;