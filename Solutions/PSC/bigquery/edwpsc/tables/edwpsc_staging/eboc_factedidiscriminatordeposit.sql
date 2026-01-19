CREATE TABLE IF NOT EXISTS edwpsc_staging.eboc_factedidiscriminatordeposit
(
  ebocedidiscriminatordepositkey INT64 NOT NULL,
  depositid INT64 NOT NULL,
  treasurybatchnumber STRING,
  creatorloginname STRING,
  creatorname STRING,
  sourceprimarykeyvalue INT64,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  createddatetime DATETIME,
  PRIMARY KEY (ebocedidiscriminatordepositkey) NOT ENFORCED
)
;