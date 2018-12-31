package com.jdm.aws.smf;

import java.util.ArrayList;
import java.util.List;

public class SearchResult extends CountResult {
	private final int total;
	private final List<Object> result = new ArrayList<>();

	public SearchResult(final Integer count, final Integer total) {
		super(count);
		this.total = total;
	}

	public int getTotal() {
		return total;
	}

	public List<?> getResult() {
		return result;
	}

	public void setResult(final List<Object> result) {
		this.result.addAll(result);
	}

	@Override
	public String toString() {
		return "SearchResult {" +
				"count=" + super.getCount() + ", " +
				"total=" + total +
				"result[" + result.size() + "]" +
				'}';
	}
}
