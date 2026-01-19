CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspelocation
(
  pspelocaddr1 STRING,
  pspelocaddr2 STRING,
  pspelocaltext STRING,
  pspeloccity STRING,
  pspeloccounty STRING,
  pspelocdesc STRING,
  pspelocecwfacid STRING,
  pspelocfax STRING,
  pspelocfulladdr STRING,
  pspelockey STRING NOT NULL,
  pspelocmgremail STRING,
  pspelocoffmgrname STRING,
  pspelocphone STRING,
  pspelocstate STRING,
  pspeloctaxid STRING,
  pspeloczip STRING,
  PRIMARY KEY (pspelockey) NOT ENFORCED
)
;