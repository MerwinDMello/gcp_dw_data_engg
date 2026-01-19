DROP TABLE `edwra_staging.ra_835_category_aggregated_temp`;

CREATE TABLE IF NOT EXISTS `edwra_staging.ra_835_category_aggregated_temp`
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  ra_claim_payment_id BIGNUMERIC(38) NOT NULL,
  ra_category_id NUMERIC(29) NOT NULL,
  amount NUMERIC(31, 2) NOT NULL,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;

DROP TABLE `edwra_staging.ra_835_category_aggregated`;

CREATE TABLE IF NOT EXISTS `edwra_staging.ra_835_category_aggregated`
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  ra_claim_payment_id BIGNUMERIC(38) NOT NULL,
  ra_category_id NUMERIC(29) NOT NULL,
  amount NUMERIC(31, 2) NOT NULL,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;