
package com.tuya.smart;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;

public class StreamingConnTest {

	public static void main(String[] args) throws Exception {
		final Options.Builder builder = new Options.Builder();
		builder.natsUrl("nats://118.89.149.174:51205");
        builder.connectWait(Duration.ofSeconds(3));
        ExecutorService executorService = Executors.newFixedThreadPool(10);
		final Counter counter = new Counter();
		final Set<Integer> topicSet = new HashSet<Integer>();
		final SecureRandom topicRandom = new SecureRandom();
		for (int i = 0; i < 10; i++) {
			executorService.submit(new Runnable() {

				public void run() {
					StreamingConnection sc = null;
					try {
						for (int i = 0; i < 50; i++) {
							int topic = Math.abs(topicRandom.nextInt());
							while (topicSet.contains(topic)) {
								topic = Math.abs(topicRandom.nextInt());
							}
							sc = NatsStreaming.connect("tuya_streaming", "test12345_sub_" + topic, builder.build());
							sc.subscribe("foo" + topic, new MessageHandler() {

								public void onMessage(Message msg) {
									System.out.println("message received=" + new String(msg.getData()));
								}
							});
							counter.increment();
						}
						System.out.println("connect counter=" + counter.value());
						Thread.sleep(1000 * 60 * 60);
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						try {
							if (sc != null) {
								sc.close();
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			});
			Thread.sleep(100);
		}
		Thread.sleep(1000 * 60 * 60);
	}

	static class Counter {

		private AtomicInteger count = new AtomicInteger(0);

		public Counter increment() {
			count.incrementAndGet();
			return this;
		}

		public int value() {
			return count.get();
		}
	}
}
