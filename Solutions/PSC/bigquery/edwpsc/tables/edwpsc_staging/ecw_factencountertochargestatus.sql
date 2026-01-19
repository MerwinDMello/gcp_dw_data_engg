CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factencountertochargestatus
(
  encountertochargestatuskey INT64 NOT NULL,
  encountertochargekey INT64 NOT NULL,
  sourcesystem STRING,
  systemstatus STRING,
  systemstartdtm DATETIME,
  systemstatusstartdtm DATETIME,
  systemcoid STRING,
  owner STRING,
  sourceprimarykeyvalue STRING NOT NULL,
  sourcerecordlastupdated DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;