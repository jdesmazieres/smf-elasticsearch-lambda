package com.jdm.aws.smf.event;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jdm.aws.smf.ProcessingUtils;
import com.opencsv.CSVIterator;
import com.opencsv.CSVReaderBuilder;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handler d'événement S3
 * Transforme un fichier BATS csv en fichiers JSon dans le bucket d'indexation
 * <p>
 * Attention, sous cette forme, le loader ne peut pas charger de gros fichiers car le contenu est chargé intégralement en mémoire
 * FIXME passer sur une api Stream
 * De plus le temps d'exécution est limité à 15 secondes ! après 15s la fonction est arrêtée est lève une erreur
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
		final LambdaLogger log = context.getLogger();
		// log.log(" [DEBUG] ----------\n" + csvBats + "\n [DEBUG] ----------\n\n");

		int noLine = 1;
		String[] headers = null;
		final CSVReaderBuilder builder = new CSVReaderBuilder(new StringReader(csvBats))
				.withSkipLines(1);
		final CSVIterator iterator = new CSVIterator(builder.build());
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

		return noLine - 1;
	}

	void processCsvRow(final String[] line, final String[] headers, final String destBucket, final String destKey, final Context context) {
		final LambdaLogger log = context.getLogger();
		final Map<String, Object> jsonMap = new HashMap<>(headers.length);
		for (int col = 0; col < headers.length; col++) {
			jsonMap.put(headers[col], line[col]);
		}
		final String id = (String) jsonMap.get(ID_COL_NAME);
		try {
			jsonMap.put("symbol", id);
			jsonMap.put("type", "Equity");
			jsonMap.put("sub-type", jsonMap.get("asset_class"));
			jsonMap.remove("asset_class");
			jsonMap.put("category", "BXESymbols");
			jsonMap.put("provider", "Bats");
			jsonMap.put("url", "https://www.batstrading.co.uk/bxe/market_data/symbol_listing/csv/");
			jsonMap.put("name", jsonMap.get("company_name"));
			jsonMap.remove("company_name");
			jsonMap.entrySet()
					.removeIf(entry -> StringUtils.isBlank((String) entry.getValue()));
			jsonMap.put("identifiers", buildIdentifiers(jsonMap));
			final String json = processingUtils.objectToJson(jsonMap);
			final String file = storeS3FileContent(destBucket, "instrument/source/bats-" + id + ".json", json);
			log.log(" [DEBUG] writeJsonFile(" + file + ")\n");
		} catch (final IOException e) {
			log.log(" [ERREUR] Erreur lors de la génération du document json: " + e.getMessage());
			throw new RuntimeException("Erreur lors de la génération du document json: " + e.getMessage(), e);
		}
	}

	private List<Map<String, String>> buildIdentifiers(final Map<String, Object> jsonMap) {
		final List<Map<String, String>> identifiers = new ArrayList<>();
		identifiers.add(buildIdentifier("isin", (String) jsonMap.get("isin")));
		identifiers.add(buildIdentifier("symbol", (String) jsonMap.get(ID_COL_NAME)));
		identifiers.add(buildIdentifier("bats", (String) jsonMap.get("bats_name")));
		identifiers.add(buildIdentifier("id", String.valueOf(System.currentTimeMillis())));
		return identifiers;
	}

	private Map<String, String> buildIdentifier(final String scheme, final String symbol) {
		final Map<String, String> ident = new HashMap<>(2);
		ident.put("scheme", scheme);
		ident.put("symbol", symbol);
		return ident;
	}

}
