CREATE OR REPLACE VIEW edwpsc_views.`pv_factencounterpayment`
AS SELECT
  `pv_factencounterpayment`.encounterpaymentkey,
  `pv_factencounterpayment`.regionkey,
  `pv_factencounterpayment`.coid,
  `pv_factencounterpayment`.clinic,
  `pv_factencounterpayment`.practice,
  `pv_factencounterpayment`.practicekey,
  `pv_factencounterpayment`.servicedatekey,
  `pv_factencounterpayment`.claimkey,
  `pv_factencounterpayment`.claimnumber,
  `pv_factencounterpayment`.encounterkey,
  `pv_factencounterpayment`.patientkey,
  `pv_factencounterpayment`.patientnumber,
  `pv_factencounterpayment`.paymentcategory,
  `pv_factencounterpayment`.paymentamt,
  `pv_factencounterpayment`.paymenttype,
  `pv_factencounterpayment`.claimdatekey,
  `pv_factencounterpayment`.visitstatus,
  `pv_factencounterpayment`.paymentcreatedatekey,
  `pv_factencounterpayment`.createdbyuserid,
  `pv_factencounterpayment`.createdbyuserkey,
  `pv_factencounterpayment`.claimcreatedflag,
  `pv_factencounterpayment`.claimbilledflag,
  `pv_factencounterpayment`.rebillflag,
  `pv_factencounterpayment`.sourceaprimarykeyvalue,
  `pv_factencounterpayment`.sourcearecordlastupdated,
  `pv_factencounterpayment`.sourcebprimarykeyvalue,
  `pv_factencounterpayment`.sourcebrecordlastupdated,
  `pv_factencounterpayment`.sourcesystemcode,
  `pv_factencounterpayment`.dwlastupdatedatetime,
  `pv_factencounterpayment`.insertedby,
  `pv_factencounterpayment`.inserteddtm,
  `pv_factencounterpayment`.modifiedby,
  `pv_factencounterpayment`.modifieddtm
  FROM
    edwpsc_base_views.`pv_factencounterpayment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factencounterpayment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;