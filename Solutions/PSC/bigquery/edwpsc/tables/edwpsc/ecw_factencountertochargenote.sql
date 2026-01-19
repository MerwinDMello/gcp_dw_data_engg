CREATE TABLE IF NOT EXISTS edwpsc.ecw_factencountertochargenote
(
  noteencountertochargekey INT64 NOT NULL,
  encountertochargekey INT64 NOT NULL,
  notename STRING,
  notestatus STRING,
  notepriority INT64,
  noteclosetomidnight STRING,
  notecreatedtm DATETIME,
  noteupdatedtm DATETIME,
  noteprovidersignednpi STRING,
  notecosignprovidernpi STRING,
  noteservicedatekey DATE,
  sourceprimarykeyvalue INT64,
  dwlastupdatedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;