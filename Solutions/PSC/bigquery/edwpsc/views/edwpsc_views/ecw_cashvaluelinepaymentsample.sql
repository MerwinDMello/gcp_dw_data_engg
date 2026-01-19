CREATE OR REPLACE VIEW edwpsc_views.`ecw_cashvaluelinepaymentsample`
AS SELECT
  `ecw_cashvaluelinepaymentsample`.claimlinechargekey,
  `ecw_cashvaluelinepaymentsample`.cptcodekey,
  `ecw_cashvaluelinepaymentsample`.cptmodifier1,
  `ecw_cashvaluelinepaymentsample`.payer1iplankey,
  `ecw_cashvaluelinepaymentsample`.payer2iplankey,
  `ecw_cashvaluelinepaymentsample`.payer3iplankey,
  `ecw_cashvaluelinepaymentsample`.coid,
  `ecw_cashvaluelinepaymentsample`.patientguarantorpatientkey,
  `ecw_cashvaluelinepaymentsample`.renderingproviderkey,
  `ecw_cashvaluelinepaymentsample`.claimkey,
  `ecw_cashvaluelinepaymentsample`.cptunits,
  `ecw_cashvaluelinepaymentsample`.servicedatekey,
  `ecw_cashvaluelinepaymentsample`.deniedclaimlineflag,
  `ecw_cashvaluelinepaymentsample`.payer1firstbilldatekey,
  `ecw_cashvaluelinepaymentsample`.payer1firstpaymentdatekey,
  `ecw_cashvaluelinepaymentsample`.payer1paymentlagdays,
  `ecw_cashvaluelinepaymentsample`.cptcharges,
  `ecw_cashvaluelinepaymentsample`.payer1contractualadjustmentamt,
  `ecw_cashvaluelinepaymentsample`.payer1allowedamt,
  `ecw_cashvaluelinepaymentsample`.payer1paymentamt,
  `ecw_cashvaluelinepaymentsample`.payer1coinsuranceamt,
  `ecw_cashvaluelinepaymentsample`.payer1memberresponsibilityamt,
  `ecw_cashvaluelinepaymentsample`.payer1deductibleamt,
  `ecw_cashvaluelinepaymentsample`.payer2paymentamt,
  `ecw_cashvaluelinepaymentsample`.feeschedulerank,
  `ecw_cashvaluelinepaymentsample`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_cashvaluelinepaymentsample`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_cashvaluelinepaymentsample`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;