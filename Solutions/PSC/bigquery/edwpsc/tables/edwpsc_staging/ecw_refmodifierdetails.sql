CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refmodifierdetails
(
  modifierdetailkey INT64 NOT NULL,
  modifiercode STRING,
  modifierdescription STRING,
  anesthesiapercentage NUMERIC(31, 2),
  anesthesiasuppressflag INT64,
  anesthesiaphysicalstatusunits INT64,
  sourcesystemcode STRING,
  sourceprimarykeyvalue STRING,
  dwlastupdatedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;