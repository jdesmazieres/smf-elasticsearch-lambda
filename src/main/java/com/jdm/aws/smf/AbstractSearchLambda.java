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
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class AbstractSearchLambda {
	private static String serviceName = "es";
	private static String region = "eu-west-3";
	private static String esDomain = "smf";
	private static String esEndpoint = "https://search-" + esDomain + "-we7mrmcmnlzf4onavhr7igonye." + region + "." + serviceName + ".amazonaws.com";
	private static String searchPath = "/instrument/_search";
	private static String countPath = "/instrument/_count";

	private static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

	Response search(final String jsonRequest, final Context context) throws IOException {
		final LambdaLogger log = context.getLogger();

		final HttpEntity entity = new NStringEntity(jsonRequest, ContentType.APPLICATION_JSON);
		final Map<String, String> params = Collections.emptyMap();
		final RestClient esClient = esClient(serviceName, region);

		return esClient.performRequest("POST", searchPath, params, entity);
	}

	Response count(final String jsonRequest, final Context context) throws IOException {
		final HttpEntity entity = new NStringEntity(jsonRequest, ContentType.APPLICATION_JSON);
		final Map<String, String> params = Collections.emptyMap();
		final RestClient esClient = esClient(serviceName, region);

		return esClient.performRequest("POST", countPath, params, entity);
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
		} catch (IOException e) {
			e.printStackTrace();
		}
		return content;
	}


	int getCount(final Response response) {
		final ObjectMapper objectMapper = new ObjectMapper();

		try {
			JsonNode rootNode = objectMapper.readTree(getContent(response));
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
