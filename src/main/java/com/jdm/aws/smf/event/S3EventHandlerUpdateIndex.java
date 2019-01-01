package com.jdm.aws.smf.event;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.jdm.aws.smf.AbstractHTTPSigningSearchLambda;
import org.elasticsearch.client.Response;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

/**
 * Handler d'événement S3
 * Indexe le document dans ElasticSearch dès qu'un fichier est déposé dans le bucket S3
 */
public class S3EventHandlerUpdateIndex
		extends AbstractHTTPSigningSearchLambda
		implements RequestHandler<S3Event, Void> {

	@Override
	public Void handleRequest(final S3Event s3event, final Context context) {
		final LambdaLogger log = context.getLogger();
		try {
			final S3EventNotification.S3EventNotificationRecord record = s3event.getRecords()
					.get(0);
			log.log("S3EventHandlerUpdateIndex: received event " + record);
			log.log("  + record: " + processingUtils.objectToJson(record));

			final String srcBucket = record.getS3()
					.getBucket()
					.getName();
			final String srcKey = getSourceFile(record);
			final String jsonDocument = getS3FileContent(srcBucket, srcKey);

			log.log("\n-----------------------------------\n" + jsonDocument + "\n--------------------------------\n");

			final String id = "0002810800";

			final Response indexResponse = index(id, jsonDocument, context);
			log.log("\n===================================\n" + indexResponse + "\n===================================\n");
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}

		return null;
	}

	private String getSourceFile(final S3EventNotification.S3EventNotificationRecord record) throws UnsupportedEncodingException {
		String srcKey = record.getS3()
				.getObject()
				.getKey()
				.replace('+', '\n');
		srcKey = URLDecoder.decode(srcKey, StandardCharsets.UTF_8.toString());
		return srcKey;
	}

	private String getS3FileContent(final String srcBucket, final String srcKey) {
		//final AmazonS3 s3Client = new AmazonS3Client();
		final AmazonS3 s3Client = AmazonS3Client.builder()
				.build();
		final S3Object s3Object = s3Client.getObject(new GetObjectRequest(
				srcBucket, srcKey));
		final InputStream objectData = s3Object.getObjectContent();
		return new BufferedReader(new InputStreamReader(objectData))
				.lines()
				.collect(Collectors.joining(" "));
	}
}
