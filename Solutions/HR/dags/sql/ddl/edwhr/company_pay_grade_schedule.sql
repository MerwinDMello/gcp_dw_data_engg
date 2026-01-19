create table if not exists {{ params.param_hr_core_dataset_name }}.company_pay_grade_schedule (
company_pay_schedule_sid int64 not null options(description="etl generated unique identifier for combination of fields like hr company id, r schedule code, schedule flag and schedule effective date")
, pay_grade_code string not null options(description="contains the grade")
, pay_step_num int64 not null options(description="contains the step if this is a step and grade schedule. for grade range schedules, step 1 is the minimum, step 2is the midpoint, and step 3 is the maximum pay rate for the grade.")
, eff_from_date date not null options(description="record effective start date")
, valid_from_date datetime not null options(description="date on which the record became valid. load date typically.")
, valid_to_date datetime options(description="date on which the record was invalidated.")
, pay_schedule_code string not null options(description="contains a set of steps and grades for a step and grade schedule, or a set of grades and minimum, midpoint, andmaximum values for a grade range schedule")
, grade_sequence_num int64 options(description="contains the sequence number that keeps  the grades in order.  grades are  alphanumeric but are not necessarily in alphabetical order")
, pay_rate_amt numeric options(description="contains the pay rate from the employee file if using step and grade. if using a grade range schedule, this is either the minimum, midpoint, or maximum")
, lawson_company_num int64 not null options(description="the number that identifies a company.a company represents a business or legal entity of an organization")
, process_level_code string not null options(description="unique process level code of an hr company value mainatined in this field.")
, active_dw_ind string options(description="y/n character to indicate this record as active in the edw.")
, source_system_code string not null options(description="a one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time datetime not null options(description="datetime of update or load of this record to the enterprise data warehouse.")
)
partition by eff_from_date
cluster by company_pay_schedule_sid, pay_grade_code, pay_step_num,  valid_from_date
options(description="it maintains details of pay schedule of details. each pay schedule is either associated with pay grade or pay grade and pay step based on its association pay rate amount is decided. ");
