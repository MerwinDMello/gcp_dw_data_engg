CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspeppipingin
(
  pspeppipgkey STRING NOT NULL,
  pspeppipgactive STRING,
  pspeppipgeffdte DATETIME,
  pspeppipggin STRING,
  pspeppipgperfid STRING,
  pspeppipgpin STRING,
  pspeppipgppiid STRING,
  pspeppipgtermdte DATETIME,
  PRIMARY KEY (pspeppipgkey) NOT ENFORCED
)
;