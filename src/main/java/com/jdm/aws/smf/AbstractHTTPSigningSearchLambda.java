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

public class AbstractHTTPSigningSearchLambda {
	protected static final String serviceName = "es";
	protected static final String region = "eu-west-3";
	protected static final String esDomain = "smf";
	protected static final String esEndpoint = "https://search-" + esDomain + "-we7mrmcmnlzf4onavhr7igonye." + region + "." + serviceName + ".amazonaws.com";
	protected static final String searchPath = "/instrument/_search";
	protected static final String countPath = "/instrument/_count";
	protected static final String indexPath = "/instrument/_doc";

	protected static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

	protected final ObjectMapper mapper = new ObjectMapper();

	protected final ProcessingUtils processingUtils = new ProcessingUtils(mapper);

	public AbstractHTTPSigningSearchLambda() {
		mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
		mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
	}

	public Response get(final String id, final Context context) throws IOException {
		final RestClient esClient = esClient(serviceName, region);
		final Request request = new Request("GET", searchPath);
		request.addParameter("q", "_id:" + id);

		return esClient.performRequest(request);
	}

	public Response search(final String jsonRequest, final Context context) throws IOException {
		final RestClient esClient = esClient(serviceName, region);
		final Request request = new Request("POST", searchPath);
		request.setJsonEntity(jsonRequest);

		return esClient.performRequest(request);
	}

	public Response count(final String jsonRequest, final Context context) throws IOException {
		final RestClient esClient = esClient(serviceName, region);
		final Request request = new Request("POST", countPath);
		request.setJsonEntity(jsonRequest);

		return esClient.performRequest(request);
	}

	public Response index(final String id, final String jsonRequest, final Context context) throws IOException {
		final RestClient esClient = esClient(serviceName, region);
		final Request request = new Request("POST", indexPath + "/" + id);
		request.setJsonEntity(jsonRequest);

		return esClient.performRequest(request);
	}

	// Adds the interceptor to the ES REST client
	public RestClient esClient(final String serviceName, final String region) {
		final AWS4Signer signer = new AWS4Signer();
		signer.setServiceName(serviceName);
		signer.setRegionName(region);
		final HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider);
		return RestClient.builder(HttpHost.create(esEndpoint))
				.setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor))
				.build();
	}

	public String getContent(final Response response) {
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

	public int getHitCount(final JsonNode rootNode) {
		final JsonNode hitsNode = rootNode.path("hits");
		final JsonNode totalNode = hitsNode.path("total");
		return totalNode.asInt();
	}
}
