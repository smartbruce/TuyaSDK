
package com.tuya.smart;

public class StreamingCounter {

	private int count = 0;

	public StreamingCounter increment() {
		count++;
		return this;
	}

	public int value() {
		return count;
	}
}
