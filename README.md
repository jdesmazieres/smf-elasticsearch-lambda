# smf-elasticsearch-lambda
Java Implementation of an aws lambda to query aws elasticSearch service

- ElasticSearch domain endpoint: https://search-smf-we7mrmcmnlzf4onavhr7igonye.eu-west-3.es.amazonaws.com
  stores au bunch of large json documents

To test locally the lambda execution, querying the 'real' aws elasticSearch service:
> mvn clear verify -P invoke
