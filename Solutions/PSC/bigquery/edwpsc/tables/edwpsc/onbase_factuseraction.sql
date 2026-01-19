CREATE TABLE IF NOT EXISTS edwpsc.onbase_factuseraction
(
  onbaseuseractionid INT64 NOT NULL,
  transactionnum INT64 NOT NULL,
  dochandleid INT64,
  actionitem STRING,
  actiondate DATETIME,
  itemname STRING,
  actionby STRING,
  actioncategory STRING,
  actionnum INT64,
  subactionnum INT64,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  PRIMARY KEY (transactionnum) NOT ENFORCED
)
;