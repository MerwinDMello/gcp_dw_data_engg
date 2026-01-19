CREATE TABLE IF NOT EXISTS edwpsc.ecw_refgeographymeditechexpanse
(
  geographykey INT64 NOT NULL,
  geographycity STRING,
  statekey STRING,
  geographyzipcode STRING,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (geographykey) NOT ENFORCED
)
;