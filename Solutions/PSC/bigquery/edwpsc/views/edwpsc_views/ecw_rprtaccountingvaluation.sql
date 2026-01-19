CREATE OR REPLACE VIEW edwpsc_views.`ecw_rprtaccountingvaluation`
AS SELECT
  `ecw_rprtaccountingvaluation`.snapshotdate,
  `ecw_rprtaccountingvaluation`.groupname,
  `ecw_rprtaccountingvaluation`.divisionname,
  `ecw_rprtaccountingvaluation`.marketname,
  `ecw_rprtaccountingvaluation`.coid,
  `ecw_rprtaccountingvaluation`.coidname,
  `ecw_rprtaccountingvaluation`.coidandcoidname,
  `ecw_rprtaccountingvaluation`.servicingprovidername,
  `ecw_rprtaccountingvaluation`.ins1payerfinancialclassnum,
  `ecw_rprtaccountingvaluation`.ins1inspayerfinancialclassname,
  `ecw_rprtaccountingvaluation`.ins1inspayergroupname,
  `ecw_rprtaccountingvaluation`.resolvedclaimcharges,
  `ecw_rprtaccountingvaluation`.resolvedcontractualadjustments,
  `ecw_rprtaccountingvaluation`.resolvedadministrationadjustments,
  `ecw_rprtaccountingvaluation`.resolveddenialadjustments,
  `ecw_rprtaccountingvaluation`.resolvedinsurancepayments,
  `ecw_rprtaccountingvaluation`.resolvedpatientpayments,
  `ecw_rprtaccountingvaluation`.unresolvedclaimchargescredit,
  `ecw_rprtaccountingvaluation`.unresolvedcontractualadjustmentscredit,
  `ecw_rprtaccountingvaluation`.unresolvedadministrationadjustmentscredit,
  `ecw_rprtaccountingvaluation`.unresolveddenialadjustmentscredit,
  `ecw_rprtaccountingvaluation`.unresolvedinsurancepaymentscredit,
  `ecw_rprtaccountingvaluation`.unresolvedpatientpaymentscredit,
  `ecw_rprtaccountingvaluation`.unresolvedinsurancebalancecredit,
  `ecw_rprtaccountingvaluation`.unresolvedpatientbalancecredit,
  `ecw_rprtaccountingvaluation`.unresolvedclaimchargesdebit,
  `ecw_rprtaccountingvaluation`.unresolvedcontractualadjustmentsdebit,
  `ecw_rprtaccountingvaluation`.unresolvedadministrationadjustmentsdebit,
  `ecw_rprtaccountingvaluation`.unresolveddenialadjustmentsdebit,
  `ecw_rprtaccountingvaluation`.unresolvedinsurancepaymentsdebit,
  `ecw_rprtaccountingvaluation`.unresolvedpatientpaymentsdebit,
  `ecw_rprtaccountingvaluation`.unresolvedinsurancebalancedebit,
  `ecw_rprtaccountingvaluation`.unresolvedpatientbalancedebit,
  `ecw_rprtaccountingvaluation`.insertedby,
  `ecw_rprtaccountingvaluation`.inserteddtm,
  `ecw_rprtaccountingvaluation`.modifiedby,
  `ecw_rprtaccountingvaluation`.modifieddtm,
  `ecw_rprtaccountingvaluation`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_rprtaccountingvaluation`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_rprtaccountingvaluation`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;