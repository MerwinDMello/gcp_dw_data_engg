SELECT TAB.name AS TableName, TAB.object_id AS ObjectID, COL.name AS ColumnName, COL.user_type_id AS DataTypeID
From sys.columns COL
INNER JOIN sys.tables TAB
On COL.object_id = TAB.object_id
-- where TAB.name = '<TABLENAME>'
-- Uncomment above line and add <Table Name> to fetch details for particular table
-- where COL.name = '<COLUMNNAME>'
-- Uncomment above line and add <Column Name> to fetch details for particular column names