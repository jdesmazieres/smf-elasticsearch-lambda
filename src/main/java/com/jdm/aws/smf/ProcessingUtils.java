package com.jdm.aws.smf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.elasticsearch.client.Response;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProcessingUtils {

	private final ObjectMapper mapper;

	public ProcessingUtils(final ObjectMapper mapper) {
		this.mapper = mapper;
	}

	public JsonNode filterResult(final JsonNode result) {
		return result.path("_source");
	}

	public CountResult buildCountResult(final Response response) throws IOException {
		final JsonNode esResponse = response2Json(response);
		final int count = esResponse.get("count")
				.asInt();
		return new CountResult(count);
	}

	public GetResult buildGetResult(final Response response) throws IOException {
		final JsonNode esResponse = response2Json(response);

		final JsonNode hitsNode = esResponse.path("hits");
		final JsonNode result = hitsNode.path("hits");

		JsonNode element = null;
		final Iterator<JsonNode> elements = result.elements();
		if (elements.hasNext()) {
			element = filterResult(elements.next());
		}

		final int count = element == null ? 0 : 1;
		final GetResult getResult = new GetResult(count);
		getResult.setResult(mapper.convertValue(element, Map.class));
		return getResult;
	}

	public SearchResult buildSearchResult(final Response response) throws IOException {
		final JsonNode esResponse = response2Json(response);

		final JsonNode hitsNode = esResponse.path("hits");
		final JsonNode result = hitsNode.path("hits");

		final ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
		final Iterator<JsonNode> elements = result.elements();
		while (elements.hasNext()) {
			arrayNode.add(filterResult(elements.next()));
		}

		final int count = result.size();
		final int total = hitsNode.path("total")
				.asInt();
		final SearchResult searchResult = new SearchResult(count, total);
		final List resultList = mapper.convertValue(arrayNode, List.class);
		searchResult.setResult(resultList);
		return searchResult;
	}

	public String object2JsonString(final Object object) throws JsonProcessingException {
		return mapper.writeValueAsString(object);
	}

	public JsonNode response2Json(final Response response) throws IOException {
		return mapper.readTree(response.getEntity()
				.getContent());
	}

	public String getResponseContent(final Response response) throws IOException {
		final String content = new BufferedReader(new InputStreamReader(response.getEntity()
				.getContent()))
				.lines()
				.collect(Collectors.joining("\n"));
		return content;
	}
}
