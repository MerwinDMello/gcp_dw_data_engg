CREATE TABLE IF NOT EXISTS edwpsc.pv_refliabilitytype
(
  liabilityownertype INT64 NOT NULL,
  liabilityownerdesc STRING NOT NULL,
  liabilityownerdesc2 STRING NOT NULL,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (liabilityownertype) NOT ENFORCED
)
;