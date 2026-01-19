
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefCoidSmryMonth AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefCoidSmryMonth AS source
ON target.SmryMonthKey = source.SmryMonthKey
WHEN MATCHED THEN
  UPDATE SET
  target.SmryMonthKey = source.SmryMonthKey,
 target.SnapShotDate = source.SnapShotDate,
 target.DisplayOrder = source.DisplayOrder,
 target.DisplayName = TRIM(source.DisplayName),
 target.LinkSnapShotDate = source.LinkSnapShotDate,
 target.LinkDateType = TRIM(source.LinkDateType),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SmryMonthKey, SnapShotDate, DisplayOrder, DisplayName, LinkSnapShotDate, LinkDateType, DWLastUpdateDateTime)
  VALUES (source.SmryMonthKey, source.SnapShotDate, source.DisplayOrder, TRIM(source.DisplayName), source.LinkSnapShotDate, TRIM(source.LinkDateType), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SmryMonthKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefCoidSmryMonth
      GROUP BY SmryMonthKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefCoidSmryMonth');
ELSE
  COMMIT TRANSACTION;
END IF;
