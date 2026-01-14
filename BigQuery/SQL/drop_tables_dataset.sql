select concat("drop table ",table_schema,".",   table_name, ";" )
from edwpi_copy.INFORMATION_SCHEMA.TABLES
order by table_name desc