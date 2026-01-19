CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspeprinsurance
(
  pspeprinagent STRING,
  pspeprincarrier STRING,
  pspeprinconemail STRING,
  pspeprinconfax STRING,
  pspeprinconname STRING,
  pspeprinconphone STRING,
  pspeprincovtype STRING,
  pspeprineffdte DATETIME,
  pspeprinexpdte DATETIME,
  pspepringafid STRING,
  pspeprininforceind STRING,
  pspeprininid STRING,
  pspeprinkey STRING NOT NULL,
  pspeprinperfid STRING,
  pspeprinpolnum STRING,
  pspeprinprimlimagg NUMERIC(29),
  pspeprinprimlimit NUMERIC(29),
  pspeprinprivlimit STRING,
  pspeprinretrodte DATETIME,
  PRIMARY KEY (pspeprinkey) NOT ENFORCED
)
;