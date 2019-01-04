package com.jdm.aws.smf.event;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jdm.aws.smf.ProcessingUtils;
import com.opencsv.CSVIterator;
import com.opencsv.CSVReader;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Handler d'événement S3
 * Transforme un fichier BATS csv en fichiers JSon dans le bucket d'indexation
 */
public class S3EventBatsFileLoader
		extends AbstractS3EventHandler {

	private static final String ID_COL_NAME = "bats_name";

	private final ObjectMapper mapper = new ObjectMapper();

	protected final ProcessingUtils processingUtils = new ProcessingUtils(mapper);

	@Override
	public void processS3File(final String srcBucket, final String srcKey, final String csvBats, final S3Event s3event, final Context context) throws Exception {
		final LambdaLogger log = context.getLogger();

		log.log(" + converting file : " + srcBucket + "/" + srcKey + " to json files to be indexed ... \n");
		final int count = convertToJsonFiles(srcBucket, "instrument/bats/", csvBats, context);
		log.log(" + converting file : " + srcBucket + "/" + srcKey + " to json files to be indexed : " + count + " json files created \n");


		log.log(" + source document file archiving ... \n");
		final String archive = archiveS3File(srcBucket, srcKey);
		log.log(" + source document file archived to: " + archive + "\n");
	}

	int convertToJsonFiles(final String destBucket, final String destKey, final String csvBats, final Context context) throws IOException {
		int noLine = 0;
		String[] headers = null;
		final CSVIterator iterator = new CSVIterator(new CSVReader(new StringReader(csvBats)));
		while (iterator.hasNext()) {
			final String[] nextLine = iterator.next();
			// skip first line
			if (noLine == 1) {
				headers = nextLine;
			} else if (noLine > 1) {
				processCsvRow(nextLine, headers, destBucket, destKey, context);
			}
			noLine++;
		}

		return noLine - 2;
	}

	void processCsvRow(final String[] line, final String[] headers, final String destBucket, final String destKey, final Context context) {
		final Map<String, String> jsonMap = new HashMap<>(headers.length);
		for (int col = 0; col < headers.length; col++) {
			jsonMap.put(headers[col], line[col]);
		}
		final String id = jsonMap.get(ID_COL_NAME);
		try {
			jsonMap.put("symbol", id);
			jsonMap.put("class", jsonMap.get("asset_class"));
			jsonMap.put("type", "BXESymbols");
			jsonMap.put("provider", "Bats");
			jsonMap.entrySet()
					.removeIf(entry -> entry.getValue() == null
							|| entry.getValue()
							.isEmpty());
			final String json = processingUtils.objectToJson(jsonMap);
			storeS3FileContent(destBucket, "instrument/source/bats-" + id + ".json", json);
		} catch (final IOException e) {
			final LambdaLogger log = context.getLogger();
			e.printStackTrace();
			log.log("Erreur lors de la génération du document json: " + e.getMessage());
		}
	}
}
