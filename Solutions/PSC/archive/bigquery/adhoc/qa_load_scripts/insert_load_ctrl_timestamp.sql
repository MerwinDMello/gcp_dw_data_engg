TRUNCATE TABLE edwpsc_ac.load_ctrl_timestamp;
INSERT INTO edwpsc_ac.load_ctrl_timestamp
SELECT * FROM prod_support.load_ctrl_timestamp;