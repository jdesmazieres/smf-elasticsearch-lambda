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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.Response;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

public class SmfInstrumentSearchPojoLambda
		extends AbstractSearchLambda {
	//implements RequestHandler<SmfInstrumentSearchPojoLambda.SearchRequest, Response> {

	//public Response handleRequest(SearchRequest searchRequest, Context context) {
	public String handleSearch(String searchRequest, Context context) {
		final LambdaLogger log = context.getLogger();

		log.log("--------------------->\n   SmfInstrumentSearchPojoLambda.search\n" + searchRequest + "\n<---------------------\n");

		Response response = null;
		try {
			response = search(searchRequest, context);
		} catch (IOException e) {
			log.log(e.getMessage());
			e.printStackTrace();
		}
		String content = getContent(response);
		//log.log("--------------------->   SmfInstrumentSearchPojoLambda.search(): \n"+content+"\n");
		int hitCount = getHitCount(content);
		log.log("==================================================================>>> total: " + hitCount + "\n");

		return content;
	}

	public String handleCount(String searchRequest, Context context) {
		final LambdaLogger log = context.getLogger();

		log.log("--------------------->\n   SmfInstrumentSearchPojoLambda.count\n" + searchRequest + "\n<---------------------\n");

		Response response = null;
		try {
			response = count(searchRequest, context);
		} catch (IOException e) {
			log.log(e.getMessage());
			e.printStackTrace();
		}
		String content = getContent(response);
		//log.log("--------------------->   SmfInstrumentSearchPojoLambda.count(): \n"+content+"\n");
		//int hitCount = getHitCount(content);
		log.log("================================================================== >>> count: " + getCount(content) + "\n");

		return content;
	}

	static class SearchRequest {

		private String query;

		public SearchRequest() {
		}

		public SearchRequest(final String queryES) {
			this.query = queryES;
		}

		public String getQuery() {
			return query;
		}

		public SearchRequest setQuery(final String query) {
			this.query = query;
			return this;
		}

	}

	static class SearchResponse {
		String jsonSearchResult;

		public SearchResponse() {
		}

		public SearchResponse(String jsonSearchResult) {
			this.jsonSearchResult = jsonSearchResult;
		}

		public String getJsonSearchResult() {
			return jsonSearchResult;
		}

		public SearchResponse setJsonSearchResult(final String jsonSearchResult) {
			this.jsonSearchResult = jsonSearchResult;
			return this;
		}
	}
}
