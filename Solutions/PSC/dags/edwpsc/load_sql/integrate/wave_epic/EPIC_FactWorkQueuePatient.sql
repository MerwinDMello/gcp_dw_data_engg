
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.EPIC_FactWorkQueuePatient ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactWorkQueuePatient (RegionKey, RegionName, Coid, WorkQueueName, EncounterKey, VisitDate, VisitCopayDue, ServicingProviderKey, ServicingProviderName, PatientKey, PatientMRN, PatientName, HoldCodeKey, HoldCode, HoldCodeName, HoldCodeDescription, TotalWorkQueueCount, DeferredStatus, ActiveWorkQueueFlag, TypeOfWorkQueue, AdminWorkQueueFlag, SourceSystemCode, SourceAPrimaryKeyValue, InsertedBy, InsertedDTM, DWLastUpdateDateTime)
SELECT source.RegionKey, TRIM(source.RegionName), TRIM(source.Coid), TRIM(source.WorkQueueName), source.EncounterKey, source.VisitDate, source.VisitCopayDue, source.ServicingProviderKey, TRIM(source.ServicingProviderName), source.PatientKey, TRIM(source.PatientMRN), TRIM(source.PatientName), source.HoldCodeKey, source.HoldCode, TRIM(source.HoldCodeName), TRIM(source.HoldCodeDescription), source.TotalWorkQueueCount, TRIM(source.DeferredStatus), TRIM(source.ActiveWorkQueueFlag), TRIM(source.TypeOfWorkQueue), TRIM(source.AdminWorkQueueFlag), TRIM(source.SourceSystemCode), TRIM(source.SourceAPrimaryKeyValue), TRIM(source.InsertedBy), source.InsertedDTM, source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.EPIC_FactWorkQueuePatient as source;
