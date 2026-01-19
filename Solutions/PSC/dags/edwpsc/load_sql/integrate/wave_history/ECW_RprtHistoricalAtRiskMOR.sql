
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtHistoricalAtRiskMOR AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtHistoricalAtRiskMOR AS source
ON target.SnapShotDate = source.SnapShotDate
WHEN MATCHED THEN
  UPDATE SET
  target.SnapShotDate = source.SnapShotDate,
 target.AtRiskSnapShotDate = source.AtRiskSnapShotDate,
 target.CenterDescription = TRIM(source.CenterDescription),
 target.Group_Name = TRIM(source.Group_Name),
 target.Division_Name = TRIM(source.Division_Name),
 target.LOB_Code = TRIM(source.LOB_Code),
 target.Coid = TRIM(source.Coid),
 target.Coid_Name = TRIM(source.Coid_Name),
 target.CoidAndName = TRIM(source.CoidAndName),
 target.CurrentMonthAtRisk = source.CurrentMonthAtRisk,
 target.PriorMonthAtRisk = source.PriorMonthAtRisk,
 target.NonParAdj = source.NonParAdj,
 target.cmDosAtRisk = source.cmDosAtRisk,
 target.Total_Balance_2MP = source.Total_Balance_2MP,
 target.Total_Balance_3MP = source.Total_Balance_3MP,
 target.Total_Balance_4MP = source.Total_Balance_4MP,
 target.AdjAmt_PM = source.AdjAmt_PM,
 target.AdjAmt_2MP = source.AdjAmt_2MP,
 target.AdjAmt_3MP = source.AdjAmt_3MP,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SnapShotDate, AtRiskSnapShotDate, CenterDescription, Group_Name, Division_Name, LOB_Code, Coid, Coid_Name, CoidAndName, CurrentMonthAtRisk, PriorMonthAtRisk, NonParAdj, cmDosAtRisk, Total_Balance_2MP, Total_Balance_3MP, Total_Balance_4MP, AdjAmt_PM, AdjAmt_2MP, AdjAmt_3MP, InsertedBy, InsertedDTM, DWLastUpdateDateTime)
  VALUES (source.SnapShotDate, source.AtRiskSnapShotDate, TRIM(source.CenterDescription), TRIM(source.Group_Name), TRIM(source.Division_Name), TRIM(source.LOB_Code), TRIM(source.Coid), TRIM(source.Coid_Name), TRIM(source.CoidAndName), source.CurrentMonthAtRisk, source.PriorMonthAtRisk, source.NonParAdj, source.cmDosAtRisk, source.Total_Balance_2MP, source.Total_Balance_3MP, source.Total_Balance_4MP, source.AdjAmt_PM, source.AdjAmt_2MP, source.AdjAmt_3MP, TRIM(source.InsertedBy), source.InsertedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtHistoricalAtRiskMOR
      GROUP BY SnapShotDate
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtHistoricalAtRiskMOR');
ELSE
  COMMIT TRANSACTION;
END IF;
