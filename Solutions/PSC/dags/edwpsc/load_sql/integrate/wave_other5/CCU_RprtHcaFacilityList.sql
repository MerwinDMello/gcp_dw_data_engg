
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.CCU_RprtHcaFacilityList ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.CCU_RprtHcaFacilityList (ContentTypeID, FacilityName, Designation, POSKey, COID, ComplianceAssetID, ID, ContentType, Modified, Created, CreatedByID, ModifiedByID, Owshiddenversion, VERSION, PATH, DWLastUpdateDateTime, InsertedBy, InsertedDTM)
SELECT TRIM(source.ContentTypeID), TRIM(source.FacilityName), TRIM(source.Designation), source.POSKey, TRIM(source.COID), TRIM(source.ComplianceAssetID), source.ID, TRIM(source.ContentType), source.Modified, source.Created, source.CreatedByID, source.ModifiedByID, source.Owshiddenversion, TRIM(source.VERSION), TRIM(source.PATH), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM
FROM {{ params.param_psc_stage_dataset_name }}.CCU_RprtHcaFacilityList as source;
