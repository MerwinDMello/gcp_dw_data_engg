CREATE TABLE IF NOT EXISTS edwpsc_staging.epic_refaccountcrosswalk
(
  accountkey INT64 NOT NULL,
  accountid INT64,
  accountinternalid INT64,
  regionkey INT64,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  accountname STRING,
  accountzcname STRING,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (accountkey) NOT ENFORCED
)
;