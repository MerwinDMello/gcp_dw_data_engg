CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspepraffiliation
(
  pspeprafactive STRING,
  pspeprafcategory STRING,
  pspepraffinishdte DATETIME,
  pspeprafgafid STRING,
  pspeprafinid STRING,
  pspeprafinstype STRING,
  pspeprafkey STRING NOT NULL,
  pspeprafperfid STRING,
  pspeprafspecialty STRING,
  pspeprafstartdte DATETIME,
  pspepraftype STRING,
  PRIMARY KEY (pspeprafkey) NOT ENFORCED
)
;