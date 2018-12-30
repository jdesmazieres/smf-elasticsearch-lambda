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
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class SmfInstrumentSearchLambda extends AbstractSearchLambda{

	public String handleCount(final String searchQuery, final Context context) throws IOException {
		final LambdaLogger log = context.getLogger();
		log.log("count('" + searchQuery + "') ...\n");

		int count = count(searchQuery, context);

		log.log("\n\n--------------------\n  -> count=" + count + "\n--------------------\n\n");

		return String.valueOf(count);
	}

	public String handleSearch(final String searchQuery, final Context context) throws IOException {
		final LambdaLogger log = context.getLogger();
		log.log("count('" + searchQuery + "') ...\n");

		String content = getContent(search(searchQuery, context));

		log.log("\n\n--------------------\n  -> content=\n" + content + "\n--------------------\n\n");

		return content;
	}

}
