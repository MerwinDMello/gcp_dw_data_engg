CREATE TABLE IF NOT EXISTS edwpsc.pk_factchangedata
(
  pkchangedatakey INT64 NOT NULL,
  createdtime DATETIME,
  modifiedtime DATETIME,
  pktransactiondetailskey INT64,
  chargeid INT64,
  fieldid INT64,
  fieldcategory STRING,
  fieldvaluetext STRING,
  fieldvalueid INT64,
  syncversion INT64,
  sourceaprimarykeyvalue INT64,
  sourcebprimarykeyvalue INT64,
  pkregionname STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;