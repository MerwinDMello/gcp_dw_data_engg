CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refnbtappspecialtycategory
(
  appspecialtycategoryid INT64 NOT NULL,
  appspecialtycategorydescription STRING NOT NULL,
  isactive BOOL NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;