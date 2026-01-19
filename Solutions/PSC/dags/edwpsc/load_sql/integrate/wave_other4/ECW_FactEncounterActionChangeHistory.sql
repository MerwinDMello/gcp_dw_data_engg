
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterActionChangeHistory AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounterActionChangeHistory AS source
ON target.ID = source.ID AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.id = source.id,
 target.encid = source.encid,
 target.logdetails = TRIM(source.logdetails),
 target.actiontype = TRIM(source.actiontype),
 target.userid = source.userid,
 target.modifydate = source.modifydate,
 target.modifiedcolumns = TRIM(source.modifiedcolumns),
 target.username = TRIM(source.username),
 target.RegionKey = source.RegionKey,
 target.LoadKey = source.LoadKey,
 target.HashNoMatch = source.HashNoMatch,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.LastChangedBy = source.LastChangedBy,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (id, encid, logdetails, actiontype, userid, modifydate, modifiedcolumns, username, RegionKey, LoadKey, HashNoMatch, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, LastChangedBy, DWLastUpdateDateTime)
  VALUES (source.id, source.encid, TRIM(source.logdetails), TRIM(source.actiontype), source.userid, source.modifydate, TRIM(source.modifiedcolumns), TRIM(source.username), source.RegionKey, source.LoadKey, source.HashNoMatch, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.LastChangedBy, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ID, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterActionChangeHistory
      GROUP BY ID, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterActionChangeHistory');
ELSE
  COMMIT TRANSACTION;
END IF;
