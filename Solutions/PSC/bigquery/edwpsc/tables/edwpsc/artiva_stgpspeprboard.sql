CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspeprboard
(
  pspeprbdcertboard STRING,
  pspeprbdcertdte DATETIME,
  pspeprbdcertmaint STRING,
  pspeprbdcertnum STRING,
  pspeprbdexpdte DATETIME,
  pspeprbdgafid STRING,
  pspeprbdkey STRING NOT NULL,
  pspeprbdlcert STRING,
  pspeprbdperfid STRING,
  pspeprbdrecertdte DATETIME,
  pspeprbdrevdte DATETIME,
  pspeprbdstatus STRING,
  PRIMARY KEY (pspeprbdkey) NOT ENFORCED
)
;