FOR tn IN (SELECT table_schema, table_name FROM edwpbs_staging.INFORMATION_SCHEMA.TABLES
Where table_type = 'BASE TABLE' AND table_name LIKE '%\\_msc%')
DO
  EXECUTE IMMEDIATE FORMAT("DROP TABLE %s.%s", tn.table_schema, tn.table_name);
END FOR;