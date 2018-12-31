package com.jdm.aws.smf;

import java.util.Map;

public class GetResult extends CountResult {
	private Map<String, Object> result;

	public GetResult(final int count) {
		super(count);
	}

	public Object getResult() {
		return result;
	}

	public void setResult(final Map<String, Object> result) {
		this.result = result;
	}

	@Override
	public String toString() {
		return "GetResult {" +
				"count=" + super.getCount() + ", " +
				"result=" + result +
				'}';
	}
}
