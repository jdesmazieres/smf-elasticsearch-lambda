package com.jdm.aws.smf;

public class CountResult {
	private final int count;

	public CountResult(final int count) {
		this.count = count;
	}

	public int getCount() {
		return count;
	}

	@Override
	public String toString() {
		return "CountResult {" +
				"count=" + count +
				'}';
	}
}
