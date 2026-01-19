CREATE OR REPLACE VIEW edwpsc_views.`ecw_refpatientguarantormeditechexpanse`
AS SELECT
  `ecw_refpatientguarantormeditechexpanse`.patientguarantorkey,
  `ecw_refpatientguarantormeditechexpanse`.regionkey,
  `ecw_refpatientguarantormeditechexpanse`.guarantornumber,
  `ecw_refpatientguarantormeditechexpanse`.relationshipcode,
  `ecw_refpatientguarantormeditechexpanse`.relationshipname,
  `ecw_refpatientguarantormeditechexpanse`.guarantorlastname,
  `ecw_refpatientguarantormeditechexpanse`.guarantornamefirst,
  `ecw_refpatientguarantormeditechexpanse`.guarantornamemiddle,
  `ecw_refpatientguarantormeditechexpanse`.guarantoraddressline1,
  `ecw_refpatientguarantormeditechexpanse`.guarantoraddressline2,
  `ecw_refpatientguarantormeditechexpanse`.guarantorgeographykey,
  `ecw_refpatientguarantormeditechexpanse`.sourceaprimarykeyvalue,
  `ecw_refpatientguarantormeditechexpanse`.sourcearecordlastupdated,
  `ecw_refpatientguarantormeditechexpanse`.sourcebprimarykeyvalue,
  `ecw_refpatientguarantormeditechexpanse`.sourcebrecordlastupdated,
  `ecw_refpatientguarantormeditechexpanse`.dwlastupdatedatetime,
  `ecw_refpatientguarantormeditechexpanse`.sourcesystemcode,
  `ecw_refpatientguarantormeditechexpanse`.insertedby,
  `ecw_refpatientguarantormeditechexpanse`.inserteddtm,
  `ecw_refpatientguarantormeditechexpanse`.modifiedby,
  `ecw_refpatientguarantormeditechexpanse`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_refpatientguarantormeditechexpanse`
;