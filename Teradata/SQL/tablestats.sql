SELECT  DatabaseName, TableName, RowCount, LastCollectTimeStamp
FROM    DBC.TableStatsV
WHERE   IndexNumber = 1
AND DataBaseName = 'edwcr'
ORDER BY RowCount DESC;