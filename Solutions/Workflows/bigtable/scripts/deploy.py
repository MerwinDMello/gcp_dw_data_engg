import datetime
import os
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import Client
from google.cloud.bigtable import enums
from google.cloud.bigtable_admin_v2.types import instance as instance_pb2
from google.protobuf import field_mask_pb2
from schema import Schema, SchemaError, Optional
import json
import argparse
import sys


def scan_files(path):
    try:
        with open(path) as fp:
            client_payload = json.load(fp)['client_payload']
        content_env_details = client_payload['environment_details']
        if 'project_id' not in content_env_details or 'instance_id' not in content_env_details:
            raise ValueError(f"Missing project_id or instance_id in environment_details for file: {path}")
        project_id = content_env_details['project_id']
        instance_id = content_env_details['instance_id']
        content_app_profiles = client_payload['data_app_profile']
        deploy_app_profile(content_app_profiles, project_id, instance_id, path)
        content_big_tables = client_payload['data_big_tables']
        deploy_bigtable_table(content_big_tables, project_id, instance_id, path)
    except Exception as e:
        print(f"[ERROR] Failed to process file {path}: {e}")
        raise
    

""" Function to deploy Bigtable app profiles """

def deploy_app_profile(data, project_id, instance_id, file_path=None):
    config_schema = Schema(
        {
            "app_profiles": [
                {
                    "app_profile": {
                        "name": str,
                        "routing_policy": str,
                        Optional("cluster_id"): str,
                        Optional("single_row_transaction"): bool,
                        Optional("multi_cluster_ids"): [str],
                        Optional("routing_priority"): str,
                    }
                }
            ],
        }
    )
    try:
        config_schema.validate(data)
    except SchemaError as er:
        print(f"[ERROR] Schema is not valid in {file_path or ''}: {er}")
        raise
    
    client = Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    instance_admin_client = client.instance_admin_client

    for ap in data["app_profiles"]:
        try:
            app_profile_id = ap["app_profile"]["name"]
            routing_policy_type = ap["app_profile"]["routing_policy"]
            routing_priority = ap["app_profile"].get("routing_priority", None)

            if routing_policy_type == "multi-cluster":
                routing_policy_type_enum = enums.RoutingPolicyType.ANY
                description = "multi-cluster routing"
                cluster_id = None
                allow_transactional_writes = None
                multi_cluster_ids = ap["app_profile"]["multi_cluster_ids"]
            elif routing_policy_type == "single-cluster":
                routing_policy_type_enum = enums.RoutingPolicyType.SINGLE
                description = "single-cluster routing"
                cluster_id = ap["app_profile"]["cluster_id"]
                allow_transactional_writes = ap["app_profile"]["single_row_transaction"]
                multi_cluster_ids = None
            else:
                print(f"[WARNING] Unknown routing_policy_type '{routing_policy_type}' for app_profile '{app_profile_id}' in {file_path or ''}. Skipping.")
                continue

            # Create app profile without priority parameter
            app_profile = instance.app_profile(
                app_profile_id=app_profile_id,
                routing_policy_type=routing_policy_type_enum,
                description=description,
                cluster_id=cluster_id,
                allow_transactional_writes=allow_transactional_writes,
                multi_cluster_ids=multi_cluster_ids,
            )

            if not app_profile.exists():
                # For new app profiles, we need to use the admin client to set priority
                if routing_priority:
                    # Build the app profile request - create first, then set fields
                    app_profile_pb = instance_pb2.AppProfile(
                        description=description
                    )

                    # Set standard_isolation priority
                    app_profile_pb.standard_isolation.priority = instance_pb2.AppProfile.Priority[routing_priority]
                    
                    # Set routing policy
                    if routing_policy_type_enum == enums.RoutingPolicyType.ANY:
                        app_profile_pb.multi_cluster_routing_use_any.cluster_ids.extend(multi_cluster_ids or [])
                    else:
                        app_profile_pb.single_cluster_routing.cluster_id = cluster_id
                        app_profile_pb.single_cluster_routing.allow_transactional_writes = allow_transactional_writes or False
                    
                    # Create using admin client
                    instance_name = instance_admin_client.instance_path(project_id, instance_id)
                    instance_admin_client.create_app_profile(
                        parent=instance_name,
                        app_profile_id=app_profile_id,
                        app_profile=app_profile_pb,
                        ignore_warnings=True
                    )
                    print(f"App profile {app_profile_id} created with priority {routing_priority}.")
                else:
                    # No priority specified, use the regular create method
                    app_profile.create(ignore_warnings=True)
                    print(f"App profile {app_profile_id} created.")
            else:
                print(f"App profile {app_profile_id} already exists.")
                
                # Fetch the current app profile from the API using admin client
                app_profile_name = instance_admin_client.app_profile_path(project_id, instance_id, app_profile_id)
                current_pb = instance_admin_client.get_app_profile(name=app_profile_name)
                needs_update = False

                # Get current priority
                current_priority = None
                if current_pb.standard_isolation and current_pb.standard_isolation.priority:
                    current_priority = instance_pb2.AppProfile.Priority(current_pb.standard_isolation.priority).name

                # Compare routing_policy_type
                current_routing_type = None
                if current_pb.multi_cluster_routing_use_any.cluster_ids:
                    current_routing_type = enums.RoutingPolicyType.ANY
                elif current_pb.single_cluster_routing.cluster_id:
                    current_routing_type = enums.RoutingPolicyType.SINGLE

                if current_routing_type != routing_policy_type_enum:
                    needs_update = True

                # Compare description
                if current_pb.description != description:
                    needs_update = True

                # Compare routing_priority
                if current_priority != routing_priority:
                    needs_update = True

                # Compare cluster_id (for single-cluster)
                if routing_policy_type == "single-cluster":
                    if current_pb.single_cluster_routing.cluster_id != cluster_id:
                        needs_update = True
                    current_txn = current_pb.single_cluster_routing.allow_transactional_writes
                    if current_txn != (allow_transactional_writes or False):
                        needs_update = True

                # Compare multi_cluster_ids (for multi-cluster)
                if routing_policy_type == "multi-cluster":
                    current_mci = list(current_pb.multi_cluster_routing_use_any.cluster_ids) if current_pb.multi_cluster_routing_use_any.cluster_ids else []
                    desired_mci = multi_cluster_ids or []
                    if sorted(current_mci) != sorted(desired_mci):
                        needs_update = True

                if needs_update:
                    print(f"Updating app profile {app_profile_id}...")

                    # Build the update request - create AppProfile first, then set fields
                    update_pb = instance_pb2.AppProfile(
                        name=app_profile_name,
                        description=description
                    )

                    # Set standard_isolation if priority is specified
                    if routing_priority:
                        update_pb.standard_isolation.priority = instance_pb2.AppProfile.Priority[routing_priority]

                    # Set routing policy
                    if routing_policy_type_enum == enums.RoutingPolicyType.ANY:
                        update_pb.multi_cluster_routing_use_any.cluster_ids.extend(multi_cluster_ids or [])
                    else:
                        update_pb.single_cluster_routing.cluster_id = cluster_id
                        update_pb.single_cluster_routing.allow_transactional_writes = allow_transactional_writes or False

                    # Update using admin client
                    update_mask = field_mask_pb2.FieldMask()
                    update_mask.paths.append("description")
                    if routing_priority:
                        update_mask.paths.append("standard_isolation")
                    if routing_policy_type_enum == enums.RoutingPolicyType.ANY:
                        update_mask.paths.append("multi_cluster_routing_use_any")
                    else:
                        update_mask.paths.append("single_cluster_routing")

                    instance_admin_client.update_app_profile(
                        app_profile=update_pb,
                        update_mask=update_mask
                    )
                    print(f"App profile {app_profile_id} updated.")
                else:
                    print(f"App profile {app_profile_id} is up to date.")
        except Exception as e:
            print(f"[ERROR] Failed to process app_profile '{ap['app_profile'].get('name', '')}' in {file_path or ''}: {e}")

""" Function to create tables """

def deploy_bigtable_table(data, project_id, instance_id, file_path=None, allow_column_family_deletion=False):
    config_schema = Schema(
        {
            "tables": [
                {
                    "table": {
                        "name": str,
                        "column_families": [
                            {"name": str, Optional("max_versions_rule"): int, Optional("max_age_rule"): int, Optional("force_recreate"): bool}
                        ],
                        Optional("allow_column_family_deletion"): bool,
                    }
                }
            ],
        }
    )
    
    try:
        config_schema.validate(data)
    except SchemaError as er:
        print(f"[ERROR] Schema is not valid in {file_path or ''}: {er}")
        raise

    # Create client and instance once per config file
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)

    # Collect all table properties and go through validation
    for tb in data["tables"]:
        try:
            table_id = tb["table"]["name"]
            table = instance.table(table_id)

            # Check if column family deletion is allowed for this specific table
            allow_cf_deletion = tb["table"].get("allow_column_family_deletion", allow_column_family_deletion)

            # Build desired column families from config
            column_families = {}
            force_recreate_cfs = set()  # Track which column families should be force recreated

            for cf in tb["table"]["column_families"]:
                max_version = int(cf["max_versions_rule"]) if "max_versions_rule" in cf else 1
                max_versions_rule = column_family.MaxVersionsGCRule(max_version)
                rules = [max_versions_rule]
                if "max_age_rule" in cf:
                    max_age = int(cf["max_age_rule"])
                    max_age_rule_ = column_family.MaxAgeGCRule(
                        datetime.timedelta(days=max_age)
                    )
                    rules.append(max_age_rule_)
                column_families[cf["name"]] = column_family.GCRuleUnion(
                    rules=rules
                )

                # Track if this column family should be force recreated
                if cf.get("force_recreate", False):
                    force_recreate_cfs.add(cf["name"])

            if not table.exists():
                table.create(column_families=column_families)
                print(f"Table {table_id} created.")
            else:
                print(f"Table {table_id} already exists. Checking for modifications...")
                existing_families = table.list_column_families()
                existing_family_names = set(existing_families.keys())
                desired_family_names = set(column_families.keys())

                # Process force recreations first (delete then will be recreated)
                for cf_name in force_recreate_cfs:
                    if cf_name in existing_families:
                        try:
                            print(f"[FORCE RECREATE] Deleting column family '{cf_name}' from table '{table_id}' for recreation...")
                            table.column_family(cf_name).delete()
                            print(f"Column family '{cf_name}' deleted successfully.")
                            # Remove from existing families so it will be recreated below
                            existing_family_names.discard(cf_name)
                        except Exception as e:
                            print(f"[ERROR] Failed to delete column family '{cf_name}' for recreation in table '{table_id}' in {file_path or ''}: {e}")
                            # Continue to try recreation anyway
                    else:
                        print(f"Column family '{cf_name}' marked for force_recreate but doesn't exist yet. Will create it.")

                # Refresh existing families after force deletions
                if force_recreate_cfs:
                    existing_families = table.list_column_families()
                    existing_family_names = set(existing_families.keys())

                # Process updates and additions
                for cf_name, new_gc_rule in column_families.items():
                    try:
                        if cf_name in existing_families:
                            existing_gc_rule = existing_families[cf_name].gc_rule
                            # Compare GC rules
                            if str(existing_gc_rule) != str(new_gc_rule):
                                print(f"[UPDATE] Updating GC rule for column family '{cf_name}' in table '{table_id}'")
                                print(f"Old rule: {existing_gc_rule}")
                                print(f"New rule: {new_gc_rule}")
                                # To update GC rule, we need to delete and recreate the column family
                                print(f"Deleting column family '{cf_name}' to update GC rule...")
                                table.column_family(cf_name).delete()
                                print(f"Recreating column family '{cf_name}' with new GC rule...")
                                cf = table.column_family(cf_name, gc_rule=new_gc_rule)
                                cf.create()
                                print(f"GC rule updated successfully.")
                            else:
                                print(f"[NO CHANGE] Column family '{cf_name}' in table '{table_id}' is already up to date.")
                        else:
                            print(f"[ADD] Column family '{cf_name}' does not exist in table '{table_id}', creating it.")
                            print(f"GC rule: {new_gc_rule}")
                            cf = table.column_family(cf_name, gc_rule=new_gc_rule)
                            cf.create()
                            print(f"Column family '{cf_name}' created successfully.")
                    except Exception as e:
                        print(f"[ERROR] Failed to process column family '{cf_name}' in table '{table_id}' in {file_path or ''}: {e}")

                # Process deletions (if allowed)
                families_to_delete = existing_family_names - desired_family_names
                if families_to_delete:
                    if allow_cf_deletion:
                        print(f"[DELETE] Found {len(families_to_delete)} column family(ies) to delete from table '{table_id}':")
                        for cf_name in families_to_delete:
                            try:
                                print(f"Deleting column family '{cf_name}'...")
                                table.column_family(cf_name).delete()
                                print(f"Column family '{cf_name}' deleted successfully.")
                            except Exception as e:
                                print(f"[ERROR] Failed to delete column family '{cf_name}' from table '{table_id}' in {file_path or ''}: {e}")
                    else:
                        print(f"[WARNING] Column families exist in table '{table_id}' but not in config: {families_to_delete}")
                        print(f"These will NOT be deleted (allow_column_family_deletion is False).")
                        print(f"To enable deletion, set 'allow_column_family_deletion': true in the table config or pass the flag to the function.")

                if not families_to_delete and existing_family_names == desired_family_names:
                    print(f"Table '{table_id}' modifications completed.")
        except Exception as e:
            print(f"[ERROR] Failed to process table '{tb['table'].get('name', '')}' in {file_path or ''}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', type=str, help='Path to the tables folder')
    args = parser.parse_args()

    error_count = 0

    print("Looking for .json files in:", os.path.join(os.getcwd(), args.path))
    print("Directory contents:", os.listdir(os.path.join(os.getcwd(), args.path)))

    files_list = [
        os.path.join(root, f)
        for root, _, files in os.walk(os.path.join(os.getcwd(), args.path))
        for f in files if f.endswith('.json')
    ]
    
    print("Files to process:", files_list) 

    for file in files_list:
        print(f"Creating bigtable resources defined in file: {file}")
        try:
            with open(file) as fp:
                print(f"File content: {fp.read()}")
            scan_files(file)
        except Exception as e:
            print(f"[ERROR] Error processing {file}: {e}")
            error_count += 1

    if error_count > 0:
        print(f"\nCompleted with {error_count} error(s).")
        sys.exit(1)
    else:
        print("\nAll files processed successfully.")
