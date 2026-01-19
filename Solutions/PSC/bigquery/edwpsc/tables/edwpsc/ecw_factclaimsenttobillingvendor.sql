CREATE TABLE IF NOT EXISTS edwpsc.ecw_factclaimsenttobillingvendor
(
  senttobillingvendorkey INT64 NOT NULL,
  claimkey INT64,
  claimnumber INT64 NOT NULL,
  regionkey INT64 NOT NULL,
  coid STRING NOT NULL,
  iplankey INT64 NOT NULL,
  senttobillingvendormessage STRING NOT NULL,
  senttobillingvendordatekey DATE NOT NULL,
  senttobillingvendordatetime TIME NOT NULL,
  sourceprimarykeyvalue INT64 NOT NULL,
  sourcerecordlastupdated DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  archivedrecord STRING NOT NULL,
  PRIMARY KEY (senttobillingvendorkey) NOT ENFORCED
)
;