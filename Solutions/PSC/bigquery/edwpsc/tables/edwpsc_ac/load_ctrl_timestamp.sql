CREATE TABLE IF NOT EXISTS edwpsc_ac.load_ctrl_timestamp
(
  table_name STRING NOT NULL,
  last_load_ctrl_timestamp DATETIME NOT NULL,
  PRIMARY KEY (table_name) NOT ENFORCED
)
CLUSTER BY table_name;