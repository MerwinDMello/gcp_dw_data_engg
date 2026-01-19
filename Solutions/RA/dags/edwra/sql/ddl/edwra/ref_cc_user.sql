-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_user.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_user
(
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  user_id BIGNUMERIC(38) NOT NULL OPTIONS(description='Identification number of the user.'),
  customer_org_id BIGNUMERIC(38) OPTIONS(description='Identification number of the customer organization.'),
  user_first_nm STRING OPTIONS(description='First name of the user.'),
  user_last_nm STRING OPTIONS(description='Last name of the user.'),
  user_title_nm STRING OPTIONS(description='Title name of the user.'),
  user_email_addr STRING OPTIONS(description='Email Address of the user.'),
  user_expire_password_ind STRING OPTIONS(description='The indicator is usually a Y/N (yes/no) designation of a record but can sometimes be other values.  This should not be 1/0 designation.'),
  user_password_expire_dt DATE OPTIONS(description='The calendar date in which user password will expire.'),
  user_is_active_ind STRING OPTIONS(description='The indicator is usually a Y/N (yes/no) designation of a record but can sometimes be other values.  This should not be 1/0 designation.'),
  user_login_id STRING OPTIONS(description='The login identifier for the user.'),
  user_role_nm STRING NOT NULL OPTIONS(description='The name of the current role for the user.'),
  user_default_summary_id BIGNUMERIC(38) NOT NULL OPTIONS(description='The user default summary identifier.'),
  user_creation_dt DATE OPTIONS(description='The calendar date the user record was created.'),
  user_created_by_id BIGNUMERIC(38) OPTIONS(description='The user identification number of the user that created the user record.'),
  user_action_emails_ind STRING NOT NULL OPTIONS(description='The indicator is usually a Y/N (yes/no) designation of a record but can sometimes be other values.  This should not be 1/0 designation.'),
  user_mod_by_user_id BIGNUMERIC(38) OPTIONS(description='The identification number of the user that last modified the user record.'),
  user_mod_date DATE OPTIONS(description='The calendar date of the last time the user record was modified.'),
  user_dual_access_ind STRING OPTIONS(description='The indicator is usually a Y/N (yes/no) designation of a record but can sometimes be other values.  This should not be 1/0 designation.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY company_code, coid, user_id
OPTIONS(
  description='Concuity User Reference table'
);
