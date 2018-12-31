# smf-elasticsearch-lambda
Java Implementation of an aws lambda to query aws elasticSearch service

- ElasticSearch domain endpoint: https://search-smf-we7mrmcmnlzf4onavhr7igonye.eu-west-3.es.amazonaws.com
  stores au bunch of large json documents

To test locally the lambda execution, querying the 'real' aws elasticSearch service:
> mvn clear verify -P invoke


# References

- Lambda Maven Archetype: https://github.com/awslabs/aws-serverless-java-archetype

- ElasticSearch client implementation: https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-request-signing.html

- ElasticSearch querying: https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-searching.html

- Creating a search application with Elastic Search: https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/search-example.html

- French documentation on lambda: https://docs.aws.amazon.com/fr_fr/lambda/latest/dg/java-author-using-eclipse-sdk-plugin.html

- ElasticSearch access policies: https://aws.amazon.com/blogs/security/how-to-control-access-to-your-amazon-elasticsearch-service-domain/

- Serverless deployment: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-deploying.html#serverless-deploying-automated

- https://forums.aws.amazon.com/thread.jspa?threadID=266642
