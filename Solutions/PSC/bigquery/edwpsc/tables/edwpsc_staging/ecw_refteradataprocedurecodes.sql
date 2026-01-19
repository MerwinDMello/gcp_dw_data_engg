CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refteradataprocedurecodes
(
  teradataprocedurecodekey INT64 NOT NULL,
  procedurecode STRING,
  proceduretypecode STRING,
  procedurecodedescription STRING,
  procedureclasssid STRING,
  procedureclassdescription STRING,
  proceduremodalitysid STRING,
  proceduremodalitydescription STRING,
  procedurespecialtysid STRING,
  procedurespecialtydescription STRING,
  effectivefromdatekey DATE,
  effectivetodatekey DATE,
  sexeditindicator STRING,
  sourcerecordlastupdated DATETIME,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;