  TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimsOnHold ;
  
  INSERT INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimsOnHold(ClaimsOnHoldKey, LoadDateKey, ClaimKey, ClaimNumber, RegionKey, HoldCodeKey, HoldCode, HoldCodeName, HoldFromDateKey, HoldToDateKey, DaysOnHold, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  SELECT source.ClaimsOnHoldKey, source.LoadDateKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, source.HoldCodeKey, source.HoldCode, TRIM(source.HoldCodeName), source.HoldFromDateKey, source.HoldToDateKey, source.DaysOnHold, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM
  FROM {{ params.param_psc_stage_dataset_name }}.EPIC_FactClaimsOnHold as source;


