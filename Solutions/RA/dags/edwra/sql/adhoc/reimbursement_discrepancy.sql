create table `edwra_staging.cc_reimbursement_discrepancy_04012025` as (
Select * from hca-hin-prod-cur-parallon.`edwra.cc_reimbursement_discrepancy` 
FOR SYSTEM_TIME AS OF TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -5 DAY));

create table `edwra_staging.cc_reimbursement_discrepancy_04022025` as (
Select * from hca-hin-prod-cur-parallon.`edwra.cc_reimbursement_discrepancy` 
FOR SYSTEM_TIME AS OF TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -4 DAY));

create table `edwra_staging.cc_reimbursement_discrepancy_04032025` as (
Select * from hca-hin-prod-cur-parallon.`edwra.cc_reimbursement_discrepancy` 
FOR SYSTEM_TIME AS OF TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -3 DAY));

create table `edwra_staging.cc_reimbursement_discrepancy_04042025` as (
Select * from hca-hin-prod-cur-parallon.`edwra.cc_reimbursement_discrepancy` 
FOR SYSTEM_TIME AS OF TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -2 DAY));

create table `edwra_staging.cc_reimbursement_discrepancy_04052025` as (
Select * from hca-hin-prod-cur-parallon.`edwra.cc_reimbursement_discrepancy` 
FOR SYSTEM_TIME AS OF TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -1 DAY));

create table `edwra_staging.cc_reimbursement_discrepancy_04062025` as (
Select * from hca-hin-prod-cur-parallon.`edwra.cc_reimbursement_discrepancy` 
FOR SYSTEM_TIME AS OF TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -0 DAY));

