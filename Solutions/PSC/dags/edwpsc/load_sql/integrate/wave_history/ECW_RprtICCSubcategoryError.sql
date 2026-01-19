
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtICCSubcategoryError AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtICCSubcategoryError AS source
ON target.SnapShotDate = source.SnapShotDate AND target.ClaimKey = source.ClaimKey
WHEN MATCHED THEN
  UPDATE SET
  target.SnapShotDate = source.SnapShotDate,
 target.ClaimKey = source.ClaimKey,
 target.COID = TRIM(source.COID),
 target.HCPatientAccountingNumber = TRIM(source.HCPatientAccountingNumber),
 target.HCENID = source.HCENID,
 target.PSSCCAT = TRIM(source.PSSCCAT),
 target.SubcategoryDescription = TRIM(source.SubcategoryDescription),
 target.PSSCID = source.PSSCID,
 target.CreateDate = source.CreateDate,
 target.CompleteFlag = TRIM(source.CompleteFlag),
 target.ClosedDate = source.ClosedDate,
 target.CurrentlyOpen = TRIM(source.CurrentlyOpen),
 target.CreatedInSnapShot = TRIM(source.CreatedInSnapShot),
 target.ClosedInSnapShot = TRIM(source.ClosedInSnapShot),
 target.Age = source.Age,
 target.OriginalRoute = TRIM(source.OriginalRoute),
 target.TotalBalanceAmt = source.TotalBalanceAmt,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.PSFAIETREROUTE = TRIM(source.PSFAIETREROUTE),
 target.PSFAIETACTDTE = TRIM(source.PSFAIETACTDTE),
 target.PSENCOIDGRP = TRIM(source.PSENCOIDGRP),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SnapShotDate, ClaimKey, COID, HCPatientAccountingNumber, HCENID, PSSCCAT, SubcategoryDescription, PSSCID, CreateDate, CompleteFlag, ClosedDate, CurrentlyOpen, CreatedInSnapShot, ClosedInSnapShot, Age, OriginalRoute, TotalBalanceAmt, ServicingProviderKey, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, PSFAIETREROUTE, PSFAIETACTDTE, PSENCOIDGRP, DWLastUpdateDateTime)
  VALUES (source.SnapShotDate, source.ClaimKey, TRIM(source.COID), TRIM(source.HCPatientAccountingNumber), source.HCENID, TRIM(source.PSSCCAT), TRIM(source.SubcategoryDescription), source.PSSCID, source.CreateDate, TRIM(source.CompleteFlag), source.ClosedDate, TRIM(source.CurrentlyOpen), TRIM(source.CreatedInSnapShot), TRIM(source.ClosedInSnapShot), source.Age, TRIM(source.OriginalRoute), source.TotalBalanceAmt, source.ServicingProviderKey, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.PSFAIETREROUTE), TRIM(source.PSFAIETACTDTE), TRIM(source.PSENCOIDGRP), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, ClaimKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtICCSubcategoryError
      GROUP BY SnapShotDate, ClaimKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtICCSubcategoryError');
ELSE
  COMMIT TRANSACTION;
END IF;
