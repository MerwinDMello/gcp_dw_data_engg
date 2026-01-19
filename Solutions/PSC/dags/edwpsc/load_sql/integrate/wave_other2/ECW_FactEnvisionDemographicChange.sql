
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEnvisionDemographicChange AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEnvisionDemographicChange AS source
ON target.EnvisionDemographicChangeKey = source.EnvisionDemographicChangeKey
WHEN MATCHED THEN
  UPDATE SET
  target.EnvisionDemographicChangeKey = source.EnvisionDemographicChangeKey,
 target.NPI = TRIM(source.NPI),
 target.LastName = TRIM(source.LastName),
 target.FirstName = TRIM(source.FirstName),
 target.MiddleName = TRIM(source.MiddleName),
 target.Suffix = TRIM(source.Suffix),
 target.Degree = TRIM(source.Degree),
 target.SiteCode = TRIM(source.SiteCode),
 target.TypeOfChange = TRIM(source.TypeOfChange),
 target.ChangeData = TRIM(source.ChangeData),
 target.ChangeEffectiveDate = source.ChangeEffectiveDate,
 target.BusinessDaysSinceLoaded = source.BusinessDaysSinceLoaded,
 target.LoadDate = source.LoadDate,
 target.Worked = source.Worked,
 target.WorkedDate = source.WorkedDate,
 target.ArtivaWorkList = source.ArtivaWorkList,
 target.ContentWorkList = source.ContentWorkList,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (EnvisionDemographicChangeKey, NPI, LastName, FirstName, MiddleName, Suffix, Degree, SiteCode, TypeOfChange, ChangeData, ChangeEffectiveDate, BusinessDaysSinceLoaded, LoadDate, Worked, WorkedDate, ArtivaWorkList, ContentWorkList, SourceSystemCode, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.EnvisionDemographicChangeKey, TRIM(source.NPI), TRIM(source.LastName), TRIM(source.FirstName), TRIM(source.MiddleName), TRIM(source.Suffix), TRIM(source.Degree), TRIM(source.SiteCode), TRIM(source.TypeOfChange), TRIM(source.ChangeData), source.ChangeEffectiveDate, source.BusinessDaysSinceLoaded, source.LoadDate, source.Worked, source.WorkedDate, source.ArtivaWorkList, source.ContentWorkList, TRIM(source.SourceSystemCode), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EnvisionDemographicChangeKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEnvisionDemographicChange
      GROUP BY EnvisionDemographicChangeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEnvisionDemographicChange');
ELSE
  COMMIT TRANSACTION;
END IF;
