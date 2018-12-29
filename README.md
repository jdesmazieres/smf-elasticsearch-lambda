# smf-elasticsearch-lambda
Java Implementation of an aws lambda to query aws elasticSearch service

- ElasticSearch domain endpoint: https://search-smf-we7mrmcmnlzf4onavhr7igonye.eu-west-3.es.amazonaws.com
  stores au bunch of large json documents

To test locally the lambda execution, querying the 'real' aws elasticSearch service:
> mvn clear verify -P invoke


# References

- ElasticSearch client implementation: https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-request-signing.html

- ElasticSearch querying: https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-searching.html

- Creating a search application with Elastic Search: https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/search-example.html
