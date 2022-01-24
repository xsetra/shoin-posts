# Create / Delete Scheduled Queries 
[Blog Post](https://shoin.cloudantler.com/bigquery-scheduled-queries/)  
Requirements.txt
```
google-cloud-bigquery-datatransfer==3.4.1
```
Execute 
```
./scheduled_queries.py gcp-project query-catalog.json create \
--service-account-name [SA] --pubsub_topic_id [TOPIC] --location us
```
