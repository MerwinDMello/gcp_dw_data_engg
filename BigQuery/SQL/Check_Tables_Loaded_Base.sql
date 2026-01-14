Select base.table_id,base.row_count rec_count, TIMESTAMP_MILLIS(base.last_modified_time), base.size_bytes / POW(10,9) AS size_gb
from edwhr.__TABLES__ base
INNER JOIN edwhr_staging_copy.copy_ctrl_table ctrl
ON base.dataset_id = ctrl.dataset
AND base.table_id = ctrl.table_name
AND appl_name = 'enwisen'
ORDER BY 3 DESC