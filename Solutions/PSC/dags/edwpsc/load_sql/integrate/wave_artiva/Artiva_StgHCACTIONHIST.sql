
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgHCACTIONHIST AS target
USING {{ params.param_psc_stage_dataset_name }}.Artiva_StgHCACTIONHIST AS source
ON target.HCACTHID = source.HCACTHID
WHEN MATCHED THEN
  UPDATE SET
  target.HCACTHID = TRIM(source.HCACTHID),
 target.HCACTHACNUM = TRIM(source.HCACTHACNUM),
 target.HCACTENCNTRID = TRIM(source.HCACTENCNTRID),
 target.HCACTHUSERID = TRIM(source.HCACTHUSERID),
 target.HCACTHDTE = source.HCACTHDTE,
 target.HCACTHTIM = source.HCACTHTIM,
 target.HCACTHACTID = TRIM(source.HCACTHACTID),
 target.HCACTHSTAT = TRIM(source.HCACTHSTAT),
 target.HCACTHRESID = TRIM(source.HCACTHRESID),
 target.HCACTHNOTE = TRIM(source.HCACTHNOTE),
 target.HCACTHNOTELINE = TRIM(source.HCACTHNOTELINE),
 target.HCACTHPOOLID = TRIM(source.HCACTHPOOLID),
 target.PSACTHAMTFLD1 = TRIM(source.PSACTHAMTFLD1),
 target.PSACTHAMTFLD2 = TRIM(source.PSACTHAMTFLD2),
 target.PSACTHAMTFLDVAL1 = TRIM(source.PSACTHAMTFLDVAL1),
 target.PSACTHAMTFLDVAL2 = TRIM(source.PSACTHAMTFLDVAL2),
 target.PSACTHDTEFLD1 = TRIM(source.PSACTHDTEFLD1),
 target.PSACTHDTEFLD2 = TRIM(source.PSACTHDTEFLD2),
 target.PSACTHDTEFLDVAL1 = TRIM(source.PSACTHDTEFLDVAL1),
 target.PSACTHDTEFLDVAL2 = TRIM(source.PSACTHDTEFLDVAL2),
 target.PSACTHFLD1 = TRIM(source.PSACTHFLD1),
 target.PSACTHFLD2 = TRIM(source.PSACTHFLD2),
 target.PSACTHFLD3 = TRIM(source.PSACTHFLD3),
 target.PSACTHFLD4 = TRIM(source.PSACTHFLD4),
 target.PSACTHFLD5 = TRIM(source.PSACTHFLD5),
 target.PSACTHFLD6 = TRIM(source.PSACTHFLD6),
 target.PSACTHFLD7 = TRIM(source.PSACTHFLD7),
 target.PSACTHFLD8 = TRIM(source.PSACTHFLD8),
 target.PSACTHFLD9 = TRIM(source.PSACTHFLD9),
 target.PSACTHFLD10 = TRIM(source.PSACTHFLD10),
 target.PSACTHFLD11 = TRIM(source.PSACTHFLD11),
 target.PSACTHFLD12 = TRIM(source.PSACTHFLD12),
 target.PSACTHFLDVAL1 = TRIM(source.PSACTHFLDVAL1),
 target.PSACTHFLDVAL2 = TRIM(source.PSACTHFLDVAL2),
 target.PSACTHFLDVAL3 = TRIM(source.PSACTHFLDVAL3),
 target.PSACTHFLDVAL4 = TRIM(source.PSACTHFLDVAL4),
 target.PSACTHFLDVAL5 = TRIM(source.PSACTHFLDVAL5),
 target.PSACTHFLDVAL6 = TRIM(source.PSACTHFLDVAL6),
 target.PSACTHFLDVAL7 = TRIM(source.PSACTHFLDVAL7),
 target.PSACTHFLDVAL8 = TRIM(source.PSACTHFLDVAL8),
 target.PSACTHFLDVAL9 = TRIM(source.PSACTHFLDVAL9),
 target.PSACTHFLDVAL10 = TRIM(source.PSACTHFLDVAL10),
 target.PSACTHFLDVAL11 = TRIM(source.PSACTHFLDVAL11),
 target.PSACTHFLDVAL12 = TRIM(source.PSACTHFLDVAL12),
 target.PSACTHUSERDEPT = TRIM(source.PSACTHUSERDEPT),
 target.PSACTHUSERDNAME = TRIM(source.PSACTHUSERDNAME),
 target.PSACTHWINDOW = TRIM(source.PSACTHWINDOW),
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.SourceSystem = TRIM(source.SourceSystem),
 target.PVClaimKey = source.PVClaimKey,
 target.PVClaimNumber = source.PVClaimNumber,
 target.PVRegionKey = source.PVRegionKey,
 target.LiabilityBal = source.LiabilityBal,
 target.PVPracticeName = TRIM(source.PVPracticeName),
 target.CurrentPayerID = TRIM(source.CurrentPayerID),
 target.SequenceNumber = source.SequenceNumber,
 target.AccountFinancialClass = TRIM(source.AccountFinancialClass),
 target.LastNote = TRIM(source.LastNote),
 target.LastNoteCnt = source.LastNoteCnt,
 target.LastNoteUserID = TRIM(source.LastNoteUserID),
 target.LastNoteDateTime = source.LastNoteDateTime,
 target.PrimaryTimelyFilingDays = source.PrimaryTimelyFilingDays,
 target.SecondaryTimelyFilingDays = source.SecondaryTimelyFilingDays,
 target.PrimaryInsFinClass = TRIM(source.PrimaryInsFinClass),
 target.DateOfService = source.DateOfService,
 target.PayerGroupName = TRIM(source.PayerGroupName),
 target.DWLastUpdateDatetime = source.DWLastUpdateDatetime
WHEN NOT MATCHED THEN
  INSERT (HCACTHID, HCACTHACNUM, HCACTENCNTRID, HCACTHUSERID, HCACTHDTE, HCACTHTIM, HCACTHACTID, HCACTHSTAT, HCACTHRESID, HCACTHNOTE, HCACTHNOTELINE, HCACTHPOOLID, PSACTHAMTFLD1, PSACTHAMTFLD2, PSACTHAMTFLDVAL1, PSACTHAMTFLDVAL2, PSACTHDTEFLD1, PSACTHDTEFLD2, PSACTHDTEFLDVAL1, PSACTHDTEFLDVAL2, PSACTHFLD1, PSACTHFLD2, PSACTHFLD3, PSACTHFLD4, PSACTHFLD5, PSACTHFLD6, PSACTHFLD7, PSACTHFLD8, PSACTHFLD9, PSACTHFLD10, PSACTHFLD11, PSACTHFLD12, PSACTHFLDVAL1, PSACTHFLDVAL2, PSACTHFLDVAL3, PSACTHFLDVAL4, PSACTHFLDVAL5, PSACTHFLDVAL6, PSACTHFLDVAL7, PSACTHFLDVAL8, PSACTHFLDVAL9, PSACTHFLDVAL10, PSACTHFLDVAL11, PSACTHFLDVAL12, PSACTHUSERDEPT, PSACTHUSERDNAME, PSACTHWINDOW, ClaimKey, ClaimNumber, RegionKey, SourceSystem, PVClaimKey, PVClaimNumber, PVRegionKey, LiabilityBal, PVPracticeName, CurrentPayerID, SequenceNumber, AccountFinancialClass, LastNote, LastNoteCnt, LastNoteUserID, LastNoteDateTime, PrimaryTimelyFilingDays, SecondaryTimelyFilingDays, PrimaryInsFinClass, DateOfService, PayerGroupName, DWLastUpdateDatetime)
  VALUES (TRIM(source.HCACTHID), TRIM(source.HCACTHACNUM), TRIM(source.HCACTENCNTRID), TRIM(source.HCACTHUSERID), source.HCACTHDTE, source.HCACTHTIM, TRIM(source.HCACTHACTID), TRIM(source.HCACTHSTAT), TRIM(source.HCACTHRESID), TRIM(source.HCACTHNOTE), TRIM(source.HCACTHNOTELINE), TRIM(source.HCACTHPOOLID), TRIM(source.PSACTHAMTFLD1), TRIM(source.PSACTHAMTFLD2), TRIM(source.PSACTHAMTFLDVAL1), TRIM(source.PSACTHAMTFLDVAL2), TRIM(source.PSACTHDTEFLD1), TRIM(source.PSACTHDTEFLD2), TRIM(source.PSACTHDTEFLDVAL1), TRIM(source.PSACTHDTEFLDVAL2), TRIM(source.PSACTHFLD1), TRIM(source.PSACTHFLD2), TRIM(source.PSACTHFLD3), TRIM(source.PSACTHFLD4), TRIM(source.PSACTHFLD5), TRIM(source.PSACTHFLD6), TRIM(source.PSACTHFLD7), TRIM(source.PSACTHFLD8), TRIM(source.PSACTHFLD9), TRIM(source.PSACTHFLD10), TRIM(source.PSACTHFLD11), TRIM(source.PSACTHFLD12), TRIM(source.PSACTHFLDVAL1), TRIM(source.PSACTHFLDVAL2), TRIM(source.PSACTHFLDVAL3), TRIM(source.PSACTHFLDVAL4), TRIM(source.PSACTHFLDVAL5), TRIM(source.PSACTHFLDVAL6), TRIM(source.PSACTHFLDVAL7), TRIM(source.PSACTHFLDVAL8), TRIM(source.PSACTHFLDVAL9), TRIM(source.PSACTHFLDVAL10), TRIM(source.PSACTHFLDVAL11), TRIM(source.PSACTHFLDVAL12), TRIM(source.PSACTHUSERDEPT), TRIM(source.PSACTHUSERDNAME), TRIM(source.PSACTHWINDOW), source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.SourceSystem), source.PVClaimKey, source.PVClaimNumber, source.PVRegionKey, source.LiabilityBal, TRIM(source.PVPracticeName), TRIM(source.CurrentPayerID), source.SequenceNumber, TRIM(source.AccountFinancialClass), TRIM(source.LastNote), source.LastNoteCnt, TRIM(source.LastNoteUserID), source.LastNoteDateTime, source.PrimaryTimelyFilingDays, source.SecondaryTimelyFilingDays, TRIM(source.PrimaryInsFinClass), source.DateOfService, TRIM(source.PayerGroupName), source.DWLastUpdateDatetime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT HCACTHID
      FROM {{ params.param_psc_core_dataset_name }}.Artiva_StgHCACTIONHIST
      GROUP BY HCACTHID
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_StgHCACTIONHIST');
ELSE
  COMMIT TRANSACTION;
END IF;
