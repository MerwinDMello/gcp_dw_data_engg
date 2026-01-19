CREATE TABLE IF NOT EXISTS edwpsc.ecw_refietv2subcategoryrank
(
  subcategoryrankkey INT64 NOT NULL,
  subcategoryid INT64,
  errortype STRING,
  errortypedescription STRING,
  subcategoryname STRING,
  ietv2subcategoryrank INT64 NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME NOT NULL
)
;