SELECT  COL.DatabaseName,
COL.TableName,
COL.ColumnName,
COL.DefaultValue
FROM    DBC.ColumnsV COL
JOIN    DBC.Tablesv TAB
ON      TAB.DatabaseName = COL.DatabaseName
AND     TAB.TableName = COL.TableName
AND     TAB.TableKind = 'T'
WHERE   COL.DataBaseName NOT IN ('All', 'Crashdumps', 'DBC', 'dbcmngr',
'Default', 'External_AP', 'EXTUSER', 'LockLogShredder', 'PUBLIC',
'Sys_Calendar', 'SysAdmin', 'SYSBAR', 'SYSJDBC', 'SYSLIB',
'SystemFe', 'SYSUDTLIB', 'SYSUIF', 'TD_SERVER_DB', 'TDStats',
'TD_SYSGPL', 'TD_SYSXML', 'TDMaps', 'TDPUSER', 'TDQCD',
'tdwm', 'SQLJ', 'TD_SYSFNLIB', 'SYSSPATIAL')
AND     COL.DefaultValue IS NOT NULL
ORDER BY    COL.DatabaseName,
COL.TableName,
COL.ColumnName;