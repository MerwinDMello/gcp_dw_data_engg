
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgUtilGroup AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgUtilGroup AS source
ON target.UtilGroupPK = source.UtilGroupPK
WHEN MATCHED THEN
  UPDATE SET
  target.Group_Name = TRIM(source.Group_Name),
 target.Entity = TRIM(source.Entity),
 target.Key_Name = TRIM(source.Key_Name),
 target.Subkey = TRIM(source.Subkey),
 target.Active = TRIM(source.Active),
 target.Text = TRIM(source.Text),
 target.Number = source.Number,
 target.Description = TRIM(source.Description),
 target.UtilGroupPK = TRIM(source.UtilGroupPK),
 target.IsUpdateable = source.IsUpdateable,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag
WHEN NOT MATCHED THEN
  INSERT (Group_Name, Entity, Key_Name, Subkey, Active, Text, Number, Description, UtilGroupPK, IsUpdateable, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag)
  VALUES (TRIM(source.Group_Name), TRIM(source.Entity), TRIM(source.Key_Name), TRIM(source.Subkey), TRIM(source.Active), TRIM(source.Text), source.Number, TRIM(source.Description), TRIM(source.UtilGroupPK), source.IsUpdateable, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT UtilGroupPK
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgUtilGroup
      GROUP BY UtilGroupPK
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgUtilGroup');
ELSE
  COMMIT TRANSACTION;
END IF;
