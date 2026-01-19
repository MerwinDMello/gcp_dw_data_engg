
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefVendorMmgt AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefVendorMmgt AS source
ON target.VendorMmgtKey = source.VendorMmgtKey
WHEN MATCHED THEN
  UPDATE SET
  target.VendorMmgtKey = source.VendorMmgtKey,
 target.SharePointID = source.SharePointID,
 target.VendorName = TRIM(source.VendorName),
 target.Title = TRIM(source.Title),
 target.SharePointModifiedDate = source.SharePointModifiedDate,
 target.SharePointCreatedDate = source.SharePointCreatedDate,
 target.CreatedByName = TRIM(source.CreatedByName),
 target.ModifiedByName = TRIM(source.ModifiedByName),
 target.RequestDate = source.RequestDate,
 target.RequestStatusValue = TRIM(source.RequestStatusValue),
 target.RequestTypeValue = TRIM(source.RequestTypeValue),
 target.UserFirstName = TRIM(source.UserFirstName),
 target.UserLastName = TRIM(source.UserLastName),
 target.User34 = TRIM(source.User34),
 target.UserEmail = TRIM(source.UserEmail),
 target.GroupValue = TRIM(source.GroupValue),
 target.RoleValue = TRIM(source.RoleValue),
 target.SubmitterEmail = TRIM(source.SubmitterEmail),
 target.CoderAAPCorAHIMALicenseNumber = TRIM(source.CoderAAPCorAHIMALicenseNumber),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime,
 target.MdInvoiceNumber = TRIM(source.MdInvoiceNumber),
 target.MdInvoiceName = TRIM(source.MdInvoiceName),
 target.MdDepartmentNumber = TRIM(source.MdDepartmentNumber),
 target.MdDepartment = TRIM(source.MdDepartment),
 target.MdDepartmentNumber2 = TRIM(source.MdDepartmentNumber2),
 target.Md2InvoiceNum = TRIM(source.Md2InvoiceNum),
 target.Md2InvoiceName = TRIM(source.Md2InvoiceName),
 target.Md2Department = TRIM(source.Md2Department),
 target.Md2DepartmentNumber = TRIM(source.Md2DepartmentNumber),
 target.Md2DepartmentNumber2 = TRIM(source.Md2DepartmentNumber2),
 target.SecondGroup = TRIM(source.SecondGroup),
 target.WFHValue = TRIM(source.WFHValue),
 target.LocationValue = TRIM(source.LocationValue),
 target.deactivationCalendar = source.deactivationCalendar,
 target.ESAFsubmit = source.ESAFsubmit,
 target.GroupChangeCount = TRIM(source.GroupChangeCount),
 target.GroupChangeLog = TRIM(source.GroupChangeLog),
 target.ESAFcomplete = source.ESAFcomplete,
 target.tempLeaveCalendar = source.tempLeaveCalendar,
 target.reactivationCalendar = source.reactivationCalendar,
 target.transferCalendar = source.transferCalendar
WHEN NOT MATCHED THEN
  INSERT (VendorMmgtKey, SharePointID, VendorName, Title, SharePointModifiedDate, SharePointCreatedDate, CreatedByName, ModifiedByName, RequestDate, RequestStatusValue, RequestTypeValue, UserFirstName, UserLastName, User34, UserEmail, GroupValue, RoleValue, SubmitterEmail, CoderAAPCorAHIMALicenseNumber, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, SysStartTime, SysEndTime, MdInvoiceNumber, MdInvoiceName, MdDepartmentNumber, MdDepartment, MdDepartmentNumber2, Md2InvoiceNum, Md2InvoiceName, Md2Department, Md2DepartmentNumber, Md2DepartmentNumber2, SecondGroup, WFHValue, LocationValue, deactivationCalendar, ESAFsubmit, GroupChangeCount, GroupChangeLog, ESAFcomplete, tempLeaveCalendar, reactivationCalendar, transferCalendar)
  VALUES (source.VendorMmgtKey, source.SharePointID, TRIM(source.VendorName), TRIM(source.Title), source.SharePointModifiedDate, source.SharePointCreatedDate, TRIM(source.CreatedByName), TRIM(source.ModifiedByName), source.RequestDate, TRIM(source.RequestStatusValue), TRIM(source.RequestTypeValue), TRIM(source.UserFirstName), TRIM(source.UserLastName), TRIM(source.User34), TRIM(source.UserEmail), TRIM(source.GroupValue), TRIM(source.RoleValue), TRIM(source.SubmitterEmail), TRIM(source.CoderAAPCorAHIMALicenseNumber), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.SysStartTime, source.SysEndTime, TRIM(source.MdInvoiceNumber), TRIM(source.MdInvoiceName), TRIM(source.MdDepartmentNumber), TRIM(source.MdDepartment), TRIM(source.MdDepartmentNumber2), TRIM(source.Md2InvoiceNum), TRIM(source.Md2InvoiceName), TRIM(source.Md2Department), TRIM(source.Md2DepartmentNumber), TRIM(source.Md2DepartmentNumber2), TRIM(source.SecondGroup), TRIM(source.WFHValue), TRIM(source.LocationValue), source.deactivationCalendar, source.ESAFsubmit, TRIM(source.GroupChangeCount), TRIM(source.GroupChangeLog), source.ESAFcomplete, source.tempLeaveCalendar, source.reactivationCalendar, source.transferCalendar);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT VendorMmgtKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefVendorMmgt
      GROUP BY VendorMmgtKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefVendorMmgt');
ELSE
  COMMIT TRANSACTION;
END IF;
