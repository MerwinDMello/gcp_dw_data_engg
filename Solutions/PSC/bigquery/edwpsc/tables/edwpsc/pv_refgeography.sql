CREATE TABLE IF NOT EXISTS edwpsc.pv_refgeography
(
  geographykey INT64 NOT NULL,
  geographycity STRING NOT NULL,
  statekey STRING NOT NULL,
  geographyzipcode STRING NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (geographykey) NOT ENFORCED
)
;