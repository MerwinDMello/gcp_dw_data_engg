CREATE TABLE IF NOT EXISTS edwpsc.epic_juncfactclaimtopayer
(
  claimkey INT64 NOT NULL,
  primaryclaimpayerkey INT64,
  secondaryclaimpayerkey INT64,
  tertiaryclaimpayerkey INT64,
  liabilityclaimpayerkey INT64,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (claimkey) NOT ENFORCED
)
;