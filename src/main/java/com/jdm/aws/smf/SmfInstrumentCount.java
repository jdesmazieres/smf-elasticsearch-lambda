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

public class SmfInstrumentCount
		extends AbstractHTTPSigningSearchLambda
		implements RequestHandler<Map<String, Object>, CountResult> {

	@Override
	public CountResult handleRequest(final Map<String, Object> countRequest, final Context context) {
		final LambdaLogger log = context.getLogger();

		log.log(" + SmfInstrumentCount.count('" + countRequest.toString() + "')\n");

		final Stopwatch swAll = Stopwatch.createStarted();
		final Stopwatch sw = Stopwatch.createStarted();
		final Response response;
		try {
			final String jsonQuery = processingUtils.objectToJson(countRequest);
			sw.reset()
					.start();
			response = count(jsonQuery, context);
			log.log("    + elasticSearch call: " + sw + "\n");
			sw.reset()
					.start();
			return processingUtils.buildCountResult(response);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		} finally {
			log.log("    + process response content: " + sw + "\n");
			log.log(" + lambda execution: " + swAll + "\n");
		}
	}
}
