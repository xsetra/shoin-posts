import json
import argparse
from google.cloud import bigquery_datatransfer


def generate_name(location, dataset_id):
    """Generates the scheduled query name
    The format is like that "{location}-{dataset_id}" if name isn't provided within file 
    Args:
        location (str): eu, us, or region name
        dataset_id (str): dataset_id provided from catalog file
    Returns:
        str: formatted name
    """
    return "{}-sq-{}".format(location, dataset_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Create or delete the scheduled queries in bigquery according to `query-catalog-file`")

    # Add arguments - POSITIONAL
    parser.add_argument('project_id', metavar='project_id', type=str, help='GCP Project id')
    parser.add_argument('query_catalog_file', metavar='query-catalog-file', type=str, help='File path the list of datasets and queries in json format, list(dict(dataset_id, query)) - list of dict')
    parser.add_argument('operation', metavar='operation', type=str, help="create or delete the scheduled queries", choices=["create", "delete"])
    # Add arguments - OPTIONAL
    parser.add_argument('--service-account-name', metavar='SERVICE_ACCOUNT_NAME', type=str, help='Assigned service account to scheduled query')
    parser.add_argument('--pubsub-topic-id', metavar='PUBSUB_TOPIC_ID', type=str, help='Destination topic for event results')
    parser.add_argument('--default-schedule', metavar='DEFAULT_SCHEDULE', type=str, help="Default schedule for query")
    parser.add_argument('--location', metavar='LOCATION', type=str, default='eu', help='Dataset location')
    
    args = parser.parse_args()
    # default_schedule = "every day 11:00"
    # location = "us"

    f = open(args.query_catalog_file)
    _ = f.read()
    query_catalog = json.loads(_)
    
    transfer_client = bigquery_datatransfer.DataTransferServiceClient()
    parent = transfer_client.common_location_path(args.project_id, args.location)
    
    if args.operation == "create": 
        for catalog in query_catalog:
            transfer_config = bigquery_datatransfer.TransferConfig(
                display_name = catalog.get('name', generate_name(args.location, catalog['dataset_id'])),
                data_source_id = "scheduled_query",
                params = {"query": catalog['query']},
                schedule = catalog.get('schedule', args.default_schedule),
                notification_pubsub_topic = catalog.get('pubsub_topic', args.pubsub_topic_id)
            )

            transfer_config = transfer_client.create_transfer_config(
                bigquery_datatransfer.CreateTransferConfigRequest(
                    parent=parent,
                    transfer_config=transfer_config,
                    service_account_name=args.service_account_name,
                )
            )
            print("Created scheduled query '{}'".format(transfer_config.name))

    elif args.operation == "delete":
        configs = list(transfer_client.list_transfer_configs(parent=parent))
        for catalog in query_catalog: 
            display_name=catalog.get('name', generate_name(args.location, catalog['dataset_id'])),
            for config in configs:
                if display_name == config.display_name:
                    transfer_client.delete_transfer_config(name=config.name)
                    print("Deleted scheduled query '{}".format(display_name))
                    