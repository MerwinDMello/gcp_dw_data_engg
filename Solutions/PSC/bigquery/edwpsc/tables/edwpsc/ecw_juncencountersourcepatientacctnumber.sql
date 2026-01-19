CREATE TABLE IF NOT EXISTS edwpsc.ecw_juncencountersourcepatientacctnumber
(
  encsourcepatacctnumberkey INT64 NOT NULL,
  regionkey INT64,
  encounterid INT64,
  sourcepatientacctnumber STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;