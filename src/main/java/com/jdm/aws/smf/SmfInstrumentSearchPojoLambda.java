/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except
 * in compliance with the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.jdm.aws.smf;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import org.elasticsearch.client.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SmfInstrumentSearchPojoLambda
		extends AbstractHTTPSigningSearchLambda
		implements RequestHandler<Map<String, Object>, Map<String, Object>> {

	@Override
	public Map<String, Object> handleRequest(final Map<String, Object> searchRequest, final Context context) {
		final LambdaLogger log = context.getLogger();

		log.log("--------------------->\n   SmfInstrumentSearchPojoLambda.search\n" + searchRequest.toString() + "\n<---------------------\n");

		final Stopwatch sw = Stopwatch.createStarted();
		Response response = null;
		try {
			final String jsonQuery = mapper.writeValueAsString(searchRequest);
			log.log("    + query object to json: " + sw + "\n");
			sw.reset()
					.start();
			response = search(jsonQuery, context);
			log.log("    + elasticSearch call: " + sw + "\n");
		} catch (final IOException e) {
			log.log(e.getMessage());
			e.printStackTrace();
		}
		sw.reset()
				.start();
		try {
			final JsonNode esResponse = mapper.readTree(response.getEntity()
					.getContent());
			final int count = getHitCount(esResponse);
			log.log("    + found: " + count + "\n");
			if (count > 0) {
				return mapper.convertValue(purgeESResponse(esResponse, count), Map.class);
			} else {
				return new HashMap<>();
			}
		} catch (final IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			log.log("    + process response content: " + sw + "\n");
		}
	}

	private JsonNode purgeESResponse(final JsonNode esResponse, final int count) {
		final ObjectNode root = mapper.createObjectNode();
		root.put("total", count);
		final JsonNode result = esResponse.path("hits")
				.path("hits");
		root.put("count", result.size());
		root.put("result", result);
		return root;
	}
}
