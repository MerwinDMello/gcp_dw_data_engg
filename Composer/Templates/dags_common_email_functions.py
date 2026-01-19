##Email utility
import os
import yaml
import logging
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# ----- Reading config from yaml file ----- #
cwd = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(cwd)
config_folder = base_dir + "/config/"
CONFIG_FILE = "email_format.yaml"
with open(config_folder + CONFIG_FILE, 'r', encoding='utf-8') as file:
    config_html = yaml.safe_load(file)


# Email HTML Generator
def email_html_generator(email_type, parameters):
    """
    generic hmtl generator function which will create alerting email with
    Metrics in tabular format.
    Inputs for this function are
    1.email_type: "success" or "failure"
    2.parameters: a dict with below mentioned format
        {
            "app_name": "<business process name>",
            "dag_name": "<dag name>",
            "current_date": "<dag start date>",
            "start_time": "<task start time>",
            "completion_time": "<task end time>",
            "env": "<execution environment>",
            "project_processing": "<processing project name>",
            "airflow_log_link": "<airflow task log link>",
            "email_subject": "<email_subject>",
        }
    """
    if email_type in config_html:
        return config_html[email_type].format(**parameters)
    else:
        print("Invaild email type!!")




class DummyEmailOperator(BaseOperator):
    @apply_defaults
    def __init__(self, to, subject, html_content, *args, **kwargs):
        super(DummyEmailOperator, self).__init__(*args, **kwargs)
        self.to = to
        self.subject = subject
        self.html_content = html_content

    def execute(self, context):
        logging.info(f"Email to: {self.to}")
        logging.info(f"Email subject: {self.subject}")
        logging.info(f"Email content: {self.html_content}")

EmailOperator = DummyEmailOperator
