SELECT 
Lower(TableName) AS TableName, TableKind
FROM dbc.tablesV 
WHERE Lower(DatabaseName) = 'edwpbs'
AND TableKind NOT IN ('V')
ORDER BY 1;