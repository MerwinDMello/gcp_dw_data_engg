CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refprovidertaxonomycode
(
  taxonomycodekey INT64 NOT NULL,
  taxonomycode STRING NOT NULL,
  specialtydesc STRING NOT NULL,
  subspecialtydec STRING NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64,
  taxonomycodedesc STRING,
  PRIMARY KEY (taxonomycodekey) NOT ENFORCED
)
;