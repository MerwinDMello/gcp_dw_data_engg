Select copy.table_id, copy.row_count copy_count, base.row_count base_count, (copy.row_count - base.row_count) diff
from edwfs_copy.__TABLES__ copy
INNER JOIN edwfs.__TABLES__ base
ON copy.table_id = base.table_id
INNER JOIN edwhr_staging_copy.copy_ctrl_table ctrl
ON base.dataset_id = ctrl.dataset
AND base.table_id = ctrl.table_name
AND appl_name = 'enwisen'