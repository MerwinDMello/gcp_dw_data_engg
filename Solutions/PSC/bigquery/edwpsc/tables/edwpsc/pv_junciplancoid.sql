CREATE TABLE IF NOT EXISTS edwpsc.pv_junciplancoid
(
  junciplancoidkey INT64 NOT NULL,
  iplankey INT64 NOT NULL,
  coid STRING NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (junciplancoidkey) NOT ENFORCED
)
;