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

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

class AbstractHTTPSigningSearchLambda {
	private static final String serviceName = "es";
	private static final String region = "eu-west-3";
	private static final String esDomain = "smf";
	private static final String esEndpoint = "https://search-" + esDomain + "-we7mrmcmnlzf4onavhr7igonye." + region + "." + serviceName + ".amazonaws.com";
	private static final String searchPath = "/instrument/_search";
	private static final String countPath = "/instrument/_count";
	private static final String getPath = "/instrument/";

	private static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();


	final ObjectMapper mapper = new ObjectMapper();

	final ProcessingUtils processingUtils = new ProcessingUtils(mapper);

	AbstractHTTPSigningSearchLambda() {
		mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
		mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
	}

	Response get(final String id, final Context context) throws IOException {
		final RestClient esClient = esClient(serviceName, region);
		final Request request = new Request("GET", searchPath);
		request.addParameter("q", "_id:" + id);

		return esClient.performRequest(request);
	}

	Response search(final String jsonRequest, final Context context) throws IOException {
		final RestClient esClient = esClient(serviceName, region);
		final Request request = new Request("POST", searchPath);
		request.setJsonEntity(jsonRequest);

		return esClient.performRequest(request);
	}

	Response count(final String jsonRequest, final Context context) throws IOException {
		final RestClient esClient = esClient(serviceName, region);
		final Request request = new Request("POST", countPath);
		request.setJsonEntity(jsonRequest);

		return esClient.performRequest(request);
	}


	// Adds the interceptor to the ES REST client
	private RestClient esClient(final String serviceName, final String region) {
		final AWS4Signer signer = new AWS4Signer();
		signer.setServiceName(serviceName);
		signer.setRegionName(region);
		final HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
		return RestClient.builder(HttpHost.create(esEndpoint))
				.setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor))
				.build();
	}

	String getContent(final Response response) {
		String content = "";
		try {
			content = new BufferedReader(new InputStreamReader(response.getEntity()
					.getContent()))
					.lines()
					.collect(Collectors.joining("\n"));
		} catch (final IOException e) {
			e.printStackTrace();
		}
		return content;
	}

	int getHitCount(final JsonNode rootNode) {
		final JsonNode hitsNode = rootNode.path("hits");
		final JsonNode totalNode = hitsNode.path("total");
		return totalNode.asInt();
	}
}
