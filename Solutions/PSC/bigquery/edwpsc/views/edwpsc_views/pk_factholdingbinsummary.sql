CREATE OR REPLACE VIEW edwpsc_views.`pk_factholdingbinsummary`
AS SELECT
  `pk_factholdingbinsummary`.holdingbinsummarykey,
  `pk_factholdingbinsummary`.pkregionname,
  `pk_factholdingbinsummary`.coid,
  `pk_factholdingbinsummary`.billingareaname,
  `pk_factholdingbinsummary`.billingproviderfirstname,
  `pk_factholdingbinsummary`.billingproviderlastname,
  `pk_factholdingbinsummary`.billingproviderusername,
  `pk_factholdingbinsummary`.patientmrn,
  `pk_factholdingbinsummary`.patientfirstname,
  `pk_factholdingbinsummary`.patientlastname,
  `pk_factholdingbinsummary`.pkfinancialclass,
  `pk_factholdingbinsummary`.servicedate,
  `pk_factholdingbinsummary`.submissiondate,
  `pk_factholdingbinsummary`.lastrevieweddate,
  `pk_factholdingbinsummary`.lastsaveddate,
  `pk_factholdingbinsummary`.visittype,
  `pk_factholdingbinsummary`.chargervu,
  `pk_factholdingbinsummary`.responsibleparty,
  `pk_factholdingbinsummary`.holdingbincategory,
  `pk_factholdingbinsummary`.holdingbinsubcategory,
  `pk_factholdingbinsummary`.cptcount,
  `pk_factholdingbinsummary`.sourceaprimarykeyvalue,
  `pk_factholdingbinsummary`.insertedby,
  `pk_factholdingbinsummary`.inserteddtm,
  `pk_factholdingbinsummary`.practiceid
  FROM
    edwpsc_base_views.`pk_factholdingbinsummary`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pk_factholdingbinsummary`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;