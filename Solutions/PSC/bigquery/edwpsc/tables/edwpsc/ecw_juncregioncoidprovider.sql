CREATE TABLE IF NOT EXISTS edwpsc.ecw_juncregioncoidprovider
(
  juncregioncoidproviderkey INT64 NOT NULL,
  regionkey INT64 NOT NULL,
  coid STRING NOT NULL,
  providerkey INT64 NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (juncregioncoidproviderkey) NOT ENFORCED
)
;