select databasename, tablename, rowcount
from edw_pub_views.approx_row_counts 
where databasename = 'edwpf'
AND tablename = 'facility_iplan';