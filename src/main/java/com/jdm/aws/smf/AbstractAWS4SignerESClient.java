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

import com.amazonaws.*;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.URI;
import java.util.stream.Collectors;

public class AbstractAWS4SignerESClient {
	static final String SERVICE_NAME = "es";
	static final String REGION = "eu-west-3";
	static final String HOST = "search-smf-we7mrmcmnlzf4onavhr7igonye." + REGION + "." + SERVICE_NAME + ".amazonaws.com";
	static final String ENDPOINT_ROOT = "https://" + HOST;
	static final String SEARCH_PATH = "/instrument/_search";
	static final String COUNT_PATH = "/instrument/_count";

	String search(final String searchQuery) {
		// Generate the request
		Request<?> request = generateRequest(COUNT_PATH, searchQuery);

		// Perform Signature Version 4 signing
		performSigningSteps(request);

		// Send the request to the server
		Response response = sendRequest(request);

		String content = getContent(response.getHttpResponse()
				.getContent());
		return content;
	}

	int count(final String searchQuery) {
		// Generate the request
		Request<?> request = generateRequest(COUNT_PATH, searchQuery);

		// Perform Signature Version 4 signing
		performSigningSteps(request);

		// Send the request to the server
		Response response = sendRequest(request);

		String content = getContent(response.getHttpResponse()
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


	Request<?> generateRequest(final String path, final String payload) {
		Request<?> request = new DefaultRequest<Void>(SERVICE_NAME);
		request.setContent(new ByteArrayInputStream(payload.getBytes()));
		request.setEndpoint(URI.create(ENDPOINT_ROOT + path));
		request.setHttpMethod(HttpMethodName.POST);
		return request;
	}


	/// Perform Signature Version 4 signing
	void performSigningSteps(Request<?> requestToSign) {
		AWS4Signer signer = new AWS4Signer();
		signer.setServiceName(SERVICE_NAME);
		signer.setRegionName(REGION);

		// Get credentials
		// NOTE: *Never* hard-code credentials in source code
		AWSCredentialsProvider credsProvider = new DefaultAWSCredentialsProviderChain();

		AWSCredentials creds = credsProvider.getCredentials();

		// Sign request with supplied creds
		signer.sign(requestToSign, creds);
	}

	Response<Void> sendRequest(Request<?> request) {
		ExecutionContext context = new ExecutionContext(true);

		ClientConfiguration clientConfiguration = new ClientConfiguration();
		AmazonHttpClient client = new AmazonHttpClient(clientConfiguration);

		MyHttpResponseHandler<Void> responseHandler = new MyHttpResponseHandler<Void>();
		MyErrorHandler errorHandler = new MyErrorHandler();

		return client.execute(request, responseHandler, errorHandler, context);
	}

	public class MyHttpResponseHandler<T> implements HttpResponseHandler<AmazonWebServiceResponse<T>> {

		@Override
		public AmazonWebServiceResponse<T> handle(
				HttpResponse response) throws Exception {

			InputStream responseStream = response.getContent();
			String responseString = getContent(responseStream);
			System.out.println(responseString);

			AmazonWebServiceResponse<T> awsResponse = new AmazonWebServiceResponse<T>();
			return awsResponse;
		}

		@Override
		public boolean needsConnectionLeftOpen() {
			return false;
		}
	}

	public class MyErrorHandler implements HttpResponseHandler<AmazonServiceException> {

		@Override
		public AmazonServiceException handle(
				HttpResponse response) throws Exception {
			System.out.println("In exception handler!");

			AmazonServiceException ase = new AmazonServiceException("Fake service exception.");
			ase.setStatusCode(response.getStatusCode());
			ase.setErrorCode(response.getStatusText());
			return ase;
		}

		@Override
		public boolean needsConnectionLeftOpen() {
			return false;
		}
	}
}
