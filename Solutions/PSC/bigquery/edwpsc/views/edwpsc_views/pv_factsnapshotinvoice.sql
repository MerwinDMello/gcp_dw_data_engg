CREATE OR REPLACE VIEW edwpsc_views.`pv_factsnapshotinvoice`
AS SELECT
  `pv_factsnapshotinvoice`.invoicesnapshotkey,
  `pv_factsnapshotinvoice`.monthid,
  `pv_factsnapshotinvoice`.snapshotdate,
  `pv_factsnapshotinvoice`.invoicekey,
  `pv_factsnapshotinvoice`.coid,
  `pv_factsnapshotinvoice`.regionkey,
  `pv_factsnapshotinvoice`.practice,
  `pv_factsnapshotinvoice`.invoicenumber,
  `pv_factsnapshotinvoice`.companynumber,
  `pv_factsnapshotinvoice`.invoicedate,
  `pv_factsnapshotinvoice`.invoicetype,
  `pv_factsnapshotinvoice`.totalchargesamt,
  `pv_factsnapshotinvoice`.totalpaymentamt,
  `pv_factsnapshotinvoice`.totaladjustmentamt,
  `pv_factsnapshotinvoice`.totalbalanceamt,
  `pv_factsnapshotinvoice`.totalendingbalanceamt,
  `pv_factsnapshotinvoice`.servicingproviderkey,
  `pv_factsnapshotinvoice`.servicingproviderid,
  `pv_factsnapshotinvoice`.referringproviderkey,
  `pv_factsnapshotinvoice`.referringproviderid,
  `pv_factsnapshotinvoice`.companyiplankey,
  `pv_factsnapshotinvoice`.companyiplanid,
  `pv_factsnapshotinvoice`.lastupdatedbyuserkey,
  `pv_factsnapshotinvoice`.lastupdatedbyuserid,
  `pv_factsnapshotinvoice`.closingdatekey,
  `pv_factsnapshotinvoice`.internalnotes,
  `pv_factsnapshotinvoice`.externalnotes,
  `pv_factsnapshotinvoice`.deleteflag,
  `pv_factsnapshotinvoice`.sourceaprimarykeyvalue,
  `pv_factsnapshotinvoice`.sourcearecordlastupdated,
  `pv_factsnapshotinvoice`.sourcebprimarykeyvalue,
  `pv_factsnapshotinvoice`.sourcebrecordlastupdated,
  `pv_factsnapshotinvoice`.dwlastupdatedatetime,
  `pv_factsnapshotinvoice`.sourcesystemcode,
  `pv_factsnapshotinvoice`.insertedby,
  `pv_factsnapshotinvoice`.inserteddtm,
  `pv_factsnapshotinvoice`.modifiedby,
  `pv_factsnapshotinvoice`.modifieddtm
  FROM
    edwpsc_base_views.`pv_factsnapshotinvoice`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factsnapshotinvoice`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;