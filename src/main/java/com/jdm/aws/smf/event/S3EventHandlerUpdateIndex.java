package com.jdm.aws.smf.event;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.fasterxml.jackson.databind.JsonNode;
import org.elasticsearch.client.Response;

import java.io.IOException;

/**
 * Handler d'événement S3
 * Indexe le document dans ElasticSearch dès qu'un fichier est déposé dans le bucket S3
 */
public class S3EventHandlerUpdateIndex
		extends AbstractS3EventHandler {

	@Override
	public void processS3File(final String srcBucket, final String srcKey, final String jsonDocument, final S3Event s3event, final Context context) throws Exception {
		final LambdaLogger log = context.getLogger();

		final String id = extractDocumentId(jsonDocument);
		log.log(" + document id [" + id + "] for file : " + srcBucket + "/" + srcKey + "\n");

		log.log(" + elasticSearch document indexing query ... \n");
		final Response indexResponse = index(id, jsonDocument, context);
		log.log(" + elasticSearch indexing query response: " + indexResponse + "\n");

		log.log(" + source document file archiving ... \n");
		final String archive = archiveS3File(srcBucket, srcKey);
		log.log(" + source document file archived to:" + archive + "\n");
	}

	private String extractDocumentId(final String jsonDocument) throws IOException {
		// FIXME performances de parser tout le document pour un seul champ !!!
		final JsonNode jsonNode = processingUtils.jsonToJsonNode(jsonDocument);
		return jsonNode.path("defaultSymbol")
				.asText();
	}
}
