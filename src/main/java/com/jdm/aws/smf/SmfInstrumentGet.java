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
import com.google.common.base.Stopwatch;
import org.elasticsearch.client.Response;

import java.io.IOException;
import java.util.Map;

public class SmfInstrumentGet
		extends AbstractHTTPSigningSearchLambda
		implements RequestHandler<Map<String, Object>, GetResult> {

	@Override
	public GetResult handleRequest(final Map<String, Object> searchRequest, final Context context) {
		final LambdaLogger log = context.getLogger();

		log.log(" + SmfInstrumentGet.get('" + searchRequest.toString() + "')\n");

		final Stopwatch swAll = Stopwatch.createStarted();
		final Stopwatch sw = Stopwatch.createStarted();
		try {
			final String id = (String) searchRequest.get("symbol");
			sw.reset()
					.start();
			final Response response = get(id, context);
			log.log("    + elasticSearch call: " + sw + "\n");
			//log.log("\n---------------------\n" + processingUtils.getResponseContent(response) + "\n<---------------------\n");
			sw.reset()
					.start();
			return processingUtils.buildGetResult(response);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		} finally {
			log.log("    + process response content: " + sw + "\n");
			log.log(" + lambda execution: " + swAll + "\n");
		}
	}
}
