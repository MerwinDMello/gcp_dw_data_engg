FOR tn IN (SELECT table_schema, table_name FROM edwpi_base_views.INFORMATION_SCHEMA.TABLES
Where table_type = 'VIEW')
DO
  EXECUTE IMMEDIATE FORMAT("DROP VIEW %s.%s", tn.table_schema, tn.table_name);
END FOR;