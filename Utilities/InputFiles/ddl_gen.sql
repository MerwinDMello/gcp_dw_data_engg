WITH t AS (
  SELECT 
    DatabaseName, 
    TableName, 
    --Remove  end of line characters if any
    RegExp_Replace(CommentString, '\r|\n', '') tablecomment 
  FROM 
    DBC.TablesV 
  WHERE 
    LOWER(DatabaseName) = {} 
    AND LOWER(TableName) IN ({})
), 
i AS (
  SELECT 
    i.DatabaseName, 
    i.TableName, 
    TRIM(
      TRAILING ',' 
      FROM 
        (
          XMLAGG(
            TRIM(columnName) || ',' 
            ORDER BY 
              columnposition
          ) (
            VARCHAR(1000)
          )
        )
    ) cl_cols 
  FROM 
    dbc.indicesV i, 
    t 
  WHERE 
    i.DatabaseName = t.DatabaseName
    AND UPPER(RIGHT(TRIM(i.DatabaseName),8)) <> '_STAGING' 
    AND i.TableName = t.TableName 
    AND IndexNumber = 1
    AND ColumnPosition <= 4 
  GROUP BY 
    1, 
    2
), 
p AS (
  SELECT 
    DatabaseName, 
    TableName, 
    CASE --use date() for 'Valid_From_Date' & 'Valid_To_Date' 
    WHEN ColumnName IN (
      'Valid_From_Date', 'Valid_To_Date'
    ) THEN 'DATE(' || ColumnName || ')' ELSE ColumnName END AS pcol 
  FROM 
    (
      SELECT 
        i.DatabaseName, 
        i.TableName, 
        ColumnName, 
        ROW_NUMBER() OVER(
          PARTITION BY i.TableName 
          ORDER BY 
            columnposition
        ) r 
      FROM 
        dbc.indicesV i, 
        t 
      WHERE 
        i.DatabaseName = t.DatabaseName
        AND UPPER(RIGHT(TRIM(i.DatabaseName),8)) <> '_STAGING' 
        AND i.TableName = t.TableName 
        AND IndexNumber = 1 
        AND (
          ColumnName LIKE '%_Date' 
          OR ColumnName LIKE 'Date_%'
        )
    ) x 
  WHERE 
    r = 1
), 
c AS (
  SELECT 
    LOWER(c.DatabaseName) DatabaseName, 
    LOWER(c.TableName) TableName, 
    LOWER(c.columnname) columnname, 
    CASE --use bigquery datatypes       
    WHEN columntype IN ('CV', 'CF', 'BO', 'BF', 'BV') THEN 'STRING' 
    WHEN columntype IN ('I', 'I1', 'I2', 'I8') THEN 'INT64' 
    WHEN columntype IN ('F ') THEN 'FLOAT64' --change valid_from_date, valid_to_date  columns to ts    
    WHEN columntype IN ('TS', 'SZ') 
    OR LOWER(c.columnname) IN (
      'valid_from_date', 'valid_to_date'
    ) THEN 'DATETIME' 
    WHEN columntype = 'DA' THEN 'DATE' 
    WHEN columntype = 'AT' THEN 'TIME'
    WHEN columntype IN ('N','D')
      THEN
      CASE 
      -- WHEN COALESCE(CAST(TRIM(decimaltotaldigits) AS INTEGER),0) BETWEEN 0 AND 18
      -- AND COALESCE(CAST(TRIM(decimalfractionaldigits) AS INTEGER),0) = 0 THEN 'INT64'
      -- WHEN UPPER(RIGHT(TRIM(c.DatabaseName),8)) = '_STAGING'
      -- AND INSTR(UPPER(c.TableName),'_WRK') = 0
      -- THEN 'FLOAT64'
      WHEN COALESCE(CAST(TRIM(decimaltotaldigits) AS INTEGER),0) < 0 THEN 'FLOAT64'
      WHEN COALESCE(CAST(TRIM(decimaltotaldigits) AS INTEGER),0) > 29 THEN 'FLOAT64'
      WHEN COALESCE(CAST(TRIM(decimalfractionaldigits) AS INTEGER),0) < 0 THEN 'FLOAT64'
      WHEN COALESCE(CAST(TRIM(decimalfractionaldigits) AS INTEGER),0) > 9 THEN 'FLOAT64' 
      ELSE 
        'NUMERIC' || '(' || TRIM(decimaltotaldigits) || ',' || TRIM(decimalfractionaldigits) || ')'
      END
    END as columntype,
    CASE WHEN nullable = 'N' THEN ' NOT NULL' ELSE '' END nullable, 
    --Remove  end of line characters if any
    RegExp_Replace(c.CommentString, '\r|\n', '') columncomment, 
    columnid 
  FROM 
    dbc.columnsV c, 
    t 
  WHERE 
    c.DatabaseName = t.DatabaseName 
    AND c.TableName = t.TableName
) 
SELECT 
  DatabaseName, 
  Lower(TableName) as table_name, 
  --First line create table with parameter name
  --part1
  --   'CREATE TABLE IF NOT EXISTS {{ params.param_' || CASE WHEN DatabaseName = 'EDWHR' THEN 'hr_core' WHEN DatabaseName = 'EDWHR_Staging' THEN 'hr_stage' END || '_dataset_name }}.' || LOWER(TableName) || ' (' || chr(13) || 
  'CREATE TABLE IF NOT EXISTS ' || Lower(DatabaseName) || '.' || LOWER(TableName) || ' (' || chr(13) || --concat all the columns including datatypes and comments of a table in the correct column order 
  --part2
  TRIM(
    TRAILING ',' 
    FROM 
      (
        XMLAGG(
          TRIM(modcolname) || ',' 
          ORDER BY 
            columnid
        ) (
          VARCHAR(22500)
        )
      )
  ) || ')' || chr(13) || --add partition clause on date columns if any
  --part3
  CASE WHEN pcol IS NOT NULL THEN 'PARTITION BY ' || pcol || chr(13) ELSE ' ' END || --add clustering clause if any
  --part4
  CASE WHEN cl_cols IS NOT NULL THEN 'CLUSTER BY ' || cl_cols || chr(13) ELSE ' ' END || --add table comments if any 
  --part5        
  CASE WHEN tablecomment IS NOT NULL THEN 'OPTIONS(description="' || tablecomment || '");' || chr(13) ELSE ';' || chr(13) END ddl 
FROM 
  (
    SELECT 
      t.DatabaseName, 
      t.TableName, 
      --concat columns including datatypes, nullabilty and comments if any
      CASE WHEN c.columncomment IS NOT NULL THEN c.columnname || ' ' || c.columntype || c.nullable || ' OPTIONS(description="' || c.columncomment || '")' || chr(13) ELSE c.columnname || ' ' || c.columntype || c.nullable || chr(13) END modcolname, 
      c.columnid, 
      i.cl_cols, 
      p.pcol, 
      t.tablecomment 
    FROM 
      t 
      INNER JOIN c ON t.DatabaseName = c.DatabaseName 
      AND t.TableName = c.TableName 
      LEFT OUTER JOIN i ON t.DatabaseName = i.DatabaseName 
      AND t.TableName = i.TableName 
      LEFT OUTER JOIN p ON t.DatabaseName = p.DatabaseName 
      AND t.TableName = p.TableName
  ) t1 
GROUP BY 
  DatabaseName, 
  TableName, 
  cl_cols, 
  pcol, 
  tablecomment 
ORDER BY 
  DatabaseName, 
  TableName;
