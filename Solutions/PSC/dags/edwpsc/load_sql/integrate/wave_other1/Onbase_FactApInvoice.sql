
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Onbase_FactApInvoice AS target
USING {{ params.param_psc_stage_dataset_name }}.Onbase_FactApInvoice AS source
ON target.OnbaseApInvoiceKey = source.OnbaseApInvoiceKey
WHEN MATCHED THEN
  UPDATE SET
  target.ItemNum = source.ItemNum,
 target.ItemTypeName = TRIM(source.ItemTypeName),
 target.AMApprovalDate = source.AMApprovalDate,
 target.APApprovalDate = source.APApprovalDate,
 target.CFOApprovalDate = source.CFOApprovalDate,
 target.FDApprovalDate = source.FDApprovalDate,
 target.GCFOApprovalDate = source.GCFOApprovalDate,
 target.GPApprovalDate = source.GPApprovalDate,
 target.HRApprovalDate = source.HRApprovalDate,
 target.MMApprovalDate = source.MMApprovalDate,
 target.OMApprovalDate = source.OMApprovalDate,
 target.PMApprovalDate = source.PMApprovalDate,
 target.VPApprovalDate = source.VPApprovalDate,
 target.InvoiceDate = source.InvoiceDate,
 target.COID = TRIM(source.COID),
 target.VendorName = TRIM(source.VendorName),
 target.VendorNumber = source.VendorNumber,
 target.Amount = source.Amount,
 target.InvoiceNumber221 = TRIM(source.InvoiceNumber221),
 target.VoucherType = TRIM(source.VoucherType),
 target.InvoiceType = TRIM(source.InvoiceType),
 target.APUserName = TRIM(source.APUserName),
 target.OMUserName = TRIM(source.OMUserName),
 target.PMUserName = TRIM(source.PMUserName),
 target.MMUserName = TRIM(source.MMUserName),
 target.VPUserName = TRIM(source.VPUserName),
 target.GCFOUserName = TRIM(source.GCFOUserName),
 target.GPUserName = TRIM(source.GPUserName),
 target.CFOUserName = TRIM(source.CFOUserName),
 target.GVPUserName = TRIM(source.GVPUserName),
 target.HRUserName = TRIM(source.HRUserName),
 target.AMUserName = TRIM(source.AMUserName),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.OnbaseApInvoiceKey = source.OnbaseApInvoiceKey
WHEN NOT MATCHED THEN
  INSERT (ItemNum, ItemTypeName, AMApprovalDate, APApprovalDate, CFOApprovalDate, FDApprovalDate, GCFOApprovalDate, GPApprovalDate, HRApprovalDate, MMApprovalDate, OMApprovalDate, PMApprovalDate, VPApprovalDate, InvoiceDate, COID, VendorName, VendorNumber, Amount, InvoiceNumber221, VoucherType, InvoiceType, APUserName, OMUserName, PMUserName, MMUserName, VPUserName, GCFOUserName, GPUserName, CFOUserName, GVPUserName, HRUserName, AMUserName, InsertedBy, InsertedDTM, SourceSystemCode, DWLastUpdateDateTime, ModifiedBy, ModifiedDTM, OnbaseApInvoiceKey)
  VALUES (source.ItemNum, TRIM(source.ItemTypeName), source.AMApprovalDate, source.APApprovalDate, source.CFOApprovalDate, source.FDApprovalDate, source.GCFOApprovalDate, source.GPApprovalDate, source.HRApprovalDate, source.MMApprovalDate, source.OMApprovalDate, source.PMApprovalDate, source.VPApprovalDate, source.InvoiceDate, TRIM(source.COID), TRIM(source.VendorName), source.VendorNumber, source.Amount, TRIM(source.InvoiceNumber221), TRIM(source.VoucherType), TRIM(source.InvoiceType), TRIM(source.APUserName), TRIM(source.OMUserName), TRIM(source.PMUserName), TRIM(source.MMUserName), TRIM(source.VPUserName), TRIM(source.GCFOUserName), TRIM(source.GPUserName), TRIM(source.CFOUserName), TRIM(source.GVPUserName), TRIM(source.HRUserName), TRIM(source.AMUserName), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.SourceSystemCode), source.DWLastUpdateDateTime, TRIM(source.ModifiedBy), source.ModifiedDTM, source.OnbaseApInvoiceKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT OnbaseApInvoiceKey
      FROM {{ params.param_psc_core_dataset_name }}.Onbase_FactApInvoice
      GROUP BY OnbaseApInvoiceKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Onbase_FactApInvoice');
ELSE
  COMMIT TRANSACTION;
END IF;
