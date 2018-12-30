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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.nio.entity.NStringEntity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class AbstractSearchLambda {
	static final String SERVICE_NAME = "es";
	static final String REGION = "eu-west-3";
	static final String ESDOMAIN = "smf";
	static final String HOST = "search-"+ESDOMAIN+"-we7mrmcmnlzf4onavhr7igonye." + REGION + "." + SERVICE_NAME + ".amazonaws.com";
	static final String ENDPOINT_ROOT = "https://" + HOST;
	static final String SEARCH_PATH = "/instrument/_search";
	static final String COUNT_PATH = "/instrument/_count";

	InputStream search(final String jsonRequest, final Context context) throws IOException {
		final LambdaLogger log = context.getLogger();
		log.log("Search: post elasticSearch url: "+ENDPOINT_ROOT + COUNT_PATH+"\n with payload:\n"+jsonRequest);


		HttpClient httpClient = new DefaultHttpClient();
		HttpPost httpPost = new HttpPost(ENDPOINT_ROOT + SEARCH_PATH);
		final HttpEntity entity = new NStringEntity(jsonRequest, ContentType.APPLICATION_JSON);
		httpPost.setEntity(entity);
		//StringEntity input = new StringEntity(jsonRequest);
		//input.setContentType("application/json");
		//httpPost.setEntity(input);

		HttpResponse httpResponse = httpClient.execute(httpPost);
		httpClient.getConnectionManager()
				.shutdown();
		return httpResponse.getEntity()
				.getContent();
	}

	int count(final String jsonRequest, final Context context) throws IOException {
		final LambdaLogger log = context.getLogger();
		log.log("Count: post elasticSearch url: "+ENDPOINT_ROOT + COUNT_PATH+"\n with payload:\n"+jsonRequest);

		HttpClient httpClient = new DefaultHttpClient();
		HttpPost httpPost = new HttpPost(ENDPOINT_ROOT + COUNT_PATH);
		final HttpEntity entity = new NStringEntity(jsonRequest, ContentType.APPLICATION_JSON);
		httpPost.setEntity(entity);
		//StringEntity input = new StringEntity(jsonRequest);
		//input.setContentType("application/json");
		//httpPost.setEntity(input);

		HttpResponse httpResponse = httpClient.execute(httpPost);
		httpClient.getConnectionManager()
				.shutdown();

		String content = getContent(httpResponse.getEntity()
				.getContent());
		return getHitCount(content);
	}

	String getContent(final InputStream responseStream) {
		String content = new BufferedReader(new InputStreamReader(responseStream))
				.lines()
				.collect(Collectors.joining("\n"));
		return content;
	}


	int getCount(final InputStream responseStream) {
		final ObjectMapper objectMapper = new ObjectMapper();

		try {
			JsonNode rootNode = objectMapper.readTree(getContent(responseStream));
			JsonNode countNode = rootNode.path("count");
			return countNode.asInt();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}

	int getHitCount(final String content) {
		final ObjectMapper objectMapper = new ObjectMapper();

		try {
			JsonNode rootNode = objectMapper.readTree(content);
			JsonNode hitsNode = rootNode.path("hits");
			JsonNode totalNode = hitsNode.path("total");
			return totalNode.asInt();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}

}
