CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_juncprovidercoid
(
  juncprovidercoid INT64 NOT NULL,
  providerkey INT64 NOT NULL,
  coid STRING NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  providertype INT64,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (juncprovidercoid) NOT ENFORCED
)
;