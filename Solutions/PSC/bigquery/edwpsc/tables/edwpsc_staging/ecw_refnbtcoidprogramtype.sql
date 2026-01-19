CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refnbtcoidprogramtype
(
  coid INT64 NOT NULL,
  programtypeid INT64,
  programtypedescription STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;