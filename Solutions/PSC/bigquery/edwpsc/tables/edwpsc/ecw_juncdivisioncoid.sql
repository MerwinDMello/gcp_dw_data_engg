CREATE TABLE IF NOT EXISTS edwpsc.ecw_juncdivisioncoid
(
  juncdivisioncoidkey INT64 NOT NULL,
  divisionkey INT64 NOT NULL,
  coid STRING NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (juncdivisioncoidkey) NOT ENFORCED
)
;