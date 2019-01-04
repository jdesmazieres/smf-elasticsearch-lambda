package com.jdm.aws.smf.event;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.*;
import com.jdm.aws.smf.AbstractHTTPSigningSearchLambda;

import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Date;
import java.util.stream.Collectors;

/**
 * Handler d'événement S3
 * Indexe le document dans ElasticSearch dès qu'un fichier est déposé dans le bucket S3
 */
public abstract class AbstractS3EventHandler
		extends AbstractHTTPSigningSearchLambda
		implements RequestHandler<S3Event, Void> {

	private static final String TARGET_DIR_PATTERN = "archive/{0,date,yyyy-MM-dd}/";

	abstract void processS3File(final String srcBucket, final String srcKey, final String srcContent, final S3Event s3event, final Context context) throws Exception;

	@Override
	public final Void handleRequest(final S3Event s3event, final Context context) {
		final LambdaLogger log = context.getLogger();
		try {
			final S3EventNotification.S3EventNotificationRecord record = s3event.getRecords()
					.get(0);
			log.log(getClass().getSimpleName() + ": received event " + record);
			log.log("  + record: " + processingUtils.objectToJson(record));

			final String srcBucket = record.getS3()
					.getBucket()
					.getName();
			final String srcKey = getSourceFile(record);

			log.log(" + loading file : " + srcBucket + "/" + srcKey + " ... \n");
			final String jsonDocument = getS3FileContent(srcBucket, srcKey);

			processS3File(srcBucket, srcKey, jsonDocument, s3event, context);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}

		return null;
	}

	String getSourceFile(final S3EventNotification.S3EventNotificationRecord record) throws UnsupportedEncodingException {
		String srcKey = record.getS3()
				.getObject()
				.getKey()
				.replace('+', '\n');
		srcKey = URLDecoder.decode(srcKey, StandardCharsets.UTF_8.toString());
		return srcKey;
	}

	String getS3FileContent(final String srcBucket, final String srcKey) {
		final AmazonS3 s3Client = AmazonS3Client.builder()
				.build();
		final S3Object s3Object = s3Client.getObject(new GetObjectRequest(
				srcBucket, srcKey));
		final InputStream objectData = s3Object.getObjectContent();
		return new BufferedReader(new InputStreamReader(objectData))
				.lines()
				.collect(Collectors.joining(" "));
	}

	void storeS3FileContent(final String srcBucket, final String srcKey, final String content) {
		final AmazonS3 s3Client = AmazonS3Client.builder()
				.build();
		final ObjectMetadata meta = new ObjectMetadata();
		meta.setContentLength(content.length());
		meta.setContentType("application/json");
		final InputStream is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
		s3Client.putObject(new PutObjectRequest(srcBucket, srcKey, is, meta));
	}

	String archiveS3File(final String srcBucket, final String srcKey) {
		final AmazonS3 s3Client = AmazonS3Client.builder()
				.build();

		// must not be a subdirectory of source directory, otherwise infinite calls
		final String targetDir = MessageFormat.format(TARGET_DIR_PATTERN, new Date());
		final String tgtKey = srcKey.replace("source/", targetDir);

		// move the object into a new object in the same bucket.
		final CopyObjectRequest copyObjRequest = new CopyObjectRequest(srcBucket, srcKey, srcBucket, tgtKey);
		s3Client.copyObject(copyObjRequest);

		final DeleteObjectRequest deleteObjRequest = new DeleteObjectRequest(srcBucket, srcKey);
		s3Client.deleteObject(deleteObjRequest);

		return srcBucket + "/" + tgtKey;
	}
}
