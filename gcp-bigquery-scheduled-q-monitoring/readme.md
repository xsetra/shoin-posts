# Review / Check BigQuery Scheduled Query Execution Results
[Blog Post](https://shoin.cloudantler.com/bigquery-scheduled-query-monitoring/)  
The reviewer actually does just checking the state is SUCCEEDED or not. Besides it publish new message to email-relay topic, if query is failed.

# Deploy
```
export TOPIC="bq-scheduled-query-results"
export SA_NAME="bq-results-reviewer"
export REGION="europe-west1"
export PROJECT=""


gcloud pubsub topics create $TOPIC
export TOPIC_ID=`gcloud pubsub topics list --filter="name.scope(topic):'${TOPIC}'" --format json  | jq -r '.[0].name'`
gcloud iam service-accounts create $SA_NAME \
    --description="Monitoring the scheduled query results" \
    --display-name="${SA_NAME}" --project $PROJECT
gcloud projects add-iam-policy-binding $PROJECT \
    --member="serviceAccount:${SA_NAME}@${PROJECT}.iam.gserviceaccount.com" \
    --role="roles/pubsub.publisher" --project $PROJECT

gcloud functions deploy bq-scheduled-query-results-reviewer --runtime python39 --source ./src/ --entry-point scheduled_query_monitoring_pubsub --env-vars-file env.yaml --trigger-topic $TOPIC_ID --region $REGION --memory 128MB --service-account "${SA_NAME}@${PROJECT}.iam.gserviceaccount.com" --max-instances 3
```