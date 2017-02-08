
package com.tuya.smart;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;

public class StreamingSubTest {

	public static void main(String[] args) throws Exception {
		final Options.Builder builder = new Options.Builder();
		builder.natsUrl("nats://118.89.149.174:51205");
		int threadSize = StreamingConstants.DEFAULT_THREAD_SIZE;
		ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
		for (int i = 0; i < threadSize; i++) {
			final int topicSuffix = i + 1;
			executorService.submit(new Runnable() {

				public void run() {
					StreamingConnection sc = null;
					final Counter counter = new Counter();
					try {
						sc = NatsStreaming.connect("tuya_streaming", "test12345_sub_" + topicSuffix, builder.build());
						sc.subscribe("streaming/" + topicSuffix, new MessageHandler() {

							public void onMessage(Message msg) {
								counter.increment();
								if (counter.value() % 100 == 0) {
									System.out.println(topicSuffix + " message received=" + counter.value());
								}
							}
						});
						Thread.sleep(1000 * 60 * 60);
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						if (sc != null) {
							try {
								sc.close();
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				}
			});
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
