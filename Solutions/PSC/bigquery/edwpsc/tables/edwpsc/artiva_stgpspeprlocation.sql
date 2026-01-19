CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspeprlocation
(
  pspeprlocactive STRING,
  pspeprlocaddrtyp STRING,
  pspeprloccoid STRING,
  pspeprlocfrmdte DATETIME,
  pspeprlocgafid STRING,
  pspeprlocgrplocid STRING,
  pspeprlockey STRING NOT NULL,
  pspeprlocperfid STRING,
  pspeprlocsilent STRING,
  pspeprloctermed STRING,
  pspeprloctodte DATETIME,
  pspeprlocloaddte DATETIME,
  pspeprlocnpi STRING,
  pspeprlocthloc STRING,
  PRIMARY KEY (pspeprlockey) NOT ENFORCED
)
;