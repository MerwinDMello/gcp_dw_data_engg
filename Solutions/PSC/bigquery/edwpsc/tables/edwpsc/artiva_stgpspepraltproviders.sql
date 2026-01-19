CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspepraltproviders
(
  pspepraltactiveind STRING,
  pspepraltdtefrm DATETIME,
  pspepraltdteto DATETIME,
  pspepraltgafid STRING,
  pspepraltid STRING,
  pspepraltkey STRING NOT NULL,
  pspepraltlongnm STRING,
  pspepraltperfid STRING,
  pspepraltprovid STRING,
  pspepralttype STRING,
  PRIMARY KEY (pspepraltkey) NOT ENFORCED
)
;