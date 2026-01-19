create table if not exists `{{ params.param_hr_core_dataset_name }}.employee_security_role`
(
  employee_34_login_code STRING NOT NULL OPTIONS(description="this field maintains employee unique network network id."),
  system_code STRING NOT NULL OPTIONS(description="this code designates the type of service (billing, payroll, etc.)"),
  security_role_code STRING NOT NULL OPTIONS(description="designates different types of access a user can have."),
  access_role_code STRING NOT NULL OPTIONS(description="describes the role of the user."),
  span_code STRING NOT NULL OPTIONS(description="category of security access an employee is provisioned to."),
  create_date DATE OPTIONS(description="date the record was created."),
  creator_user_id_code STRING OPTIONS(description="contains either the 3/4 id or lawson id of the creator."),
  last_update_date DATE OPTIONS(description="last date a record was updated in lawson."),
  active_ind STRING OPTIONS(description="a/i character to indicate this record as active in the edw."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY employee_34_login_code, system_code, security_role_code, access_role_code
OPTIONS(
  description="this tables maps each user to a span code based on a 3/4 id."
);