
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RprtCashMgmtArUnBalanced ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtCashMgmtArUnBalanced (coid, posteddate, ARSystemName, ARSystemTypeID, ARPostedAmount, CmtPostedAmount, siteID, LastNote, NoteDate, UserID, FullName, ArUnbalancedKey, DWLastUpdateDateTime, Reasons, SelectedCOIDUser)
SELECT source.coid, TRIM(source.posteddate), TRIM(source.ARSystemName), source.ARSystemTypeID, source.ARPostedAmount, source.CmtPostedAmount, TRIM(source.siteID), TRIM(source.LastNote), source.NoteDate, TRIM(source.UserID), TRIM(source.FullName), source.ArUnbalancedKey, source.DWLastUpdateDateTime, TRIM(source.Reasons), TRIM(source.SelectedCOIDUser)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RprtCashMgmtArUnBalanced as source;
