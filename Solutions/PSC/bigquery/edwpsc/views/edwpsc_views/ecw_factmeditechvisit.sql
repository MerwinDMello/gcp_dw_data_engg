CREATE OR REPLACE VIEW edwpsc_views.`ecw_factmeditechvisit`
AS SELECT
  `ecw_factmeditechvisit`.meditechvisitkey,
  `ecw_factmeditechvisit`.sendingapplication,
  `ecw_factmeditechvisit`.uniquechargeidentifier,
  `ecw_factmeditechvisit`.facilityid,
  `ecw_factmeditechvisit`.practiceid,
  `ecw_factmeditechvisit`.mrn,
  `ecw_factmeditechvisit`.visitnumber,
  `ecw_factmeditechvisit`.admitdate,
  `ecw_factmeditechvisit`.dischargedate,
  `ecw_factmeditechvisit`.patientname,
  `ecw_factmeditechvisit`.billingprovidername,
  `ecw_factmeditechvisit`.coid,
  `ecw_factmeditechvisit`.sourcesystemcode,
  `ecw_factmeditechvisit`.dwlastupdatedatetime,
  `ecw_factmeditechvisit`.insertedby,
  `ecw_factmeditechvisit`.inserteddtm,
  `ecw_factmeditechvisit`.modifiedby,
  `ecw_factmeditechvisit`.modifieddtm,
  `ecw_factmeditechvisit`.patientage,
  `ecw_factmeditechvisit`.dealicensenumber,
  `ecw_factmeditechvisit`.providerusername,
  `ecw_factmeditechvisit`.npi,
  `ecw_factmeditechvisit`.visitlocationunit,
  `ecw_factmeditechvisit`.censuscoid
  FROM
    edwpsc_base_views.`ecw_factmeditechvisit`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factmeditechvisit`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;