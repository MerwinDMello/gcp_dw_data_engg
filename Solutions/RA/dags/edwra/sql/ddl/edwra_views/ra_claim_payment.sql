Create or Replace view {{ params.param_parallon_ra_views_dataset_name }}.ra_claim_payment
as
SELECT * FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment;
