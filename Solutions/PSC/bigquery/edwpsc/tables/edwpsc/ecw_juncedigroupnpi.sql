CREATE TABLE IF NOT EXISTS edwpsc.ecw_juncedigroupnpi
(
  ecwedigroupnpikey INT64 NOT NULL,
  regionkey INT64 NOT NULL,
  npirulename STRING,
  groupnpi STRING,
  deleteflag INT64 NOT NULL,
  allapptfacilities INT64,
  allpracticingproviders INT64,
  selectiveservicedates INT64,
  startservicedate DATETIME,
  endservicedate DATETIME,
  allfacilities INT64,
  allpracticingfacilities INT64,
  allnonpracticingfacilities INT64,
  selectiveinsurances INT64,
  sourceprimarykeyvalue INT64,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;