
package com.tuya.smart;

import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;

public class StreamingSubTest {

	public static void main(String[] args) throws Exception {
		final Options.Builder builder = new Options.Builder();
		builder.natsUrl("nats://118.89.149.174:51205");
		StreamingConnection sc = NatsStreaming.connect("tuya_streaming", "test12345_sub", builder.build());
		final Counter counter = new Counter();
		try {
			sc.subscribe("foo", new MessageHandler() {

				public void onMessage(Message msg) {
					counter.increment();
					if (counter.value() % 100 == 0) {
						System.out.println("message received=" + counter.value());
					}
				}
			});
			Thread.sleep(1000 * 60 * 60);
		} finally {
			sc.close();
		}
	}

	static class Counter {

		private int count = 0;

		public Counter increment() {
			count++;
			return this;
		}

		public int value() {
			return count;
		}
	}
}
