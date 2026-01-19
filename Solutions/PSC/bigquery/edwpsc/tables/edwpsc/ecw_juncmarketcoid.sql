CREATE TABLE IF NOT EXISTS edwpsc.ecw_juncmarketcoid
(
  juncmarketcoidkey INT64 NOT NULL,
  marketkey INT64 NOT NULL,
  coid STRING NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (juncmarketcoidkey) NOT ENFORCED
)
;