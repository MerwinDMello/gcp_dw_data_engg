
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgNotes AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgNotes AS source
ON target.NotesPK = source.NotesPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.Practice = TRIM(source.Practice),
 target.Group_Name = TRIM(source.Group_Name),
 target.TYPE = TRIM(source.TYPE),
 target.Key_Value = TRIM(source.Key_Value),
 target.Subkey = TRIM(source.Subkey),
 target.Notes = TRIM(source.Notes),
 target.Active = TRIM(source.Active),
 target.Last_Upd_UserID = TRIM(source.Last_Upd_UserID),
 target.Last_Upd_DateTime = source.Last_Upd_DateTime,
 target.NotesPK = TRIM(source.NotesPK),
 target.NotesPK_txt = TRIM(source.NotesPK_txt),
 target.PatInfoPk = TRIM(source.PatInfoPk),
 target.CreatedBy = TRIM(source.CreatedBy),
 target.CreatedOn = source.CreatedOn,
 target.RegionKey = source.RegionKey,
 target.sysstarttime = source.sysstarttime,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag
WHEN NOT MATCHED THEN
  INSERT (Practice, Group_Name, TYPE, Key_Value, Subkey, Notes, Active, Last_Upd_UserID, Last_Upd_DateTime, NotesPK, NotesPK_txt, PatInfoPk, CreatedBy, CreatedOn, RegionKey, sysstarttime, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag)
  VALUES (TRIM(source.Practice), TRIM(source.Group_Name), TRIM(source.TYPE), TRIM(source.Key_Value), TRIM(source.Subkey), TRIM(source.Notes), TRIM(source.Active), TRIM(source.Last_Upd_UserID), source.Last_Upd_DateTime, TRIM(source.NotesPK), TRIM(source.NotesPK_txt), TRIM(source.PatInfoPk), TRIM(source.CreatedBy), source.CreatedOn, source.RegionKey, source.sysstarttime, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT NotesPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgNotes
      GROUP BY NotesPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgNotes');
ELSE
  COMMIT TRANSACTION;
END IF;
