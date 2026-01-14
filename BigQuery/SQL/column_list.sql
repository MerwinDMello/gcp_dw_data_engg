  SELECT
    column_name
  FROM
    `hca-hin-dev-cur-parallon`.edwclm.INFORMATION_SCHEMA.COLUMNS
  WHERE
    table_name = 'fact_charge'
  ORDER BY ordinal_position ASC;