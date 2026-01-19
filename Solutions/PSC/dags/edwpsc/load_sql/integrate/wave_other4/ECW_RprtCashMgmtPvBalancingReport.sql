
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RprtCashMgmtPvBalancingReport ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtCashMgmtPvBalancingReport (practice, COID, posteddate, PVAmount, CmtPostedAmount, Variance, PVType, PVTypeID, SpecialistFullName, LastNote, NoteDate, UserID, PvBalancingReportKey, DWLastUpdateDateTime, ReasonNames, SelectedCOIDUser)
SELECT TRIM(source.practice), TRIM(source.COID), TRIM(source.posteddate), source.PVAmount, source.CmtPostedAmount, source.Variance, TRIM(source.PVType), source.PVTypeID, TRIM(source.SpecialistFullName), TRIM(source.LastNote), source.NoteDate, TRIM(source.UserID), source.PvBalancingReportKey, source.DWLastUpdateDateTime, TRIM(source.ReasonNames), TRIM(source.SelectedCOIDUser)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RprtCashMgmtPvBalancingReport as source;
