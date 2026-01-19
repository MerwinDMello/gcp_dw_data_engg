
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.EPIC_FactWorkQueueClaimEdit ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactWorkQueueClaimEdit (RegionKey, RegionName, Coid, WorkqueueName, ClaimKey, POSCode, VisitNumber, TransactionNumber, InvoiceNumber, CPTCode, ServiceDate, DateCreated, PayorName, AmountDue, BillingProvider, PatientName, PatientMRN, RevLocation, HoldCodeKey, HoldCode, HoldCodeName, HoldCodeDescription, ErrorMessage, SourceAPrimaryKeyValue, SourceSystemCode, InsertedBy, InsertedDTM, DWLastUpdateDateTime)
SELECT source.RegionKey, TRIM(source.RegionName), TRIM(source.Coid), TRIM(source.WorkqueueName), source.ClaimKey, TRIM(source.POSCode), source.VisitNumber, source.TransactionNumber, TRIM(source.InvoiceNumber), TRIM(source.CPTCode), source.ServiceDate, source.DateCreated, TRIM(source.PayorName), source.AmountDue, TRIM(source.BillingProvider), TRIM(source.PatientName), TRIM(source.PatientMRN), TRIM(source.RevLocation), source.HoldCodeKey, TRIM(source.HoldCode), TRIM(source.HoldCodeName), TRIM(source.HoldCodeDescription), TRIM(source.ErrorMessage), TRIM(source.SourceAPrimaryKeyValue), TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, source.DWLastUpdateDateTime
FROM {{ params.param_psc_stage_dataset_name }}.EPIC_FactWorkQueueClaimEdit as source;
