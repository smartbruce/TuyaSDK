
package com.tuya.smart;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;

/**
 * 多个client订阅同一个topic
 */
public class MultiSubStreamingPubTest {

	public static void main(String[] args) throws Exception {
		final int messageSize = 10000;
		final CountDownLatch msgSizeLatch = new CountDownLatch(messageSize);
		final Options.Builder builder = new Options.Builder();
		builder.natsUrl("nats://118.89.149.174:51205");

		Random random = new Random();
		final String pubTopicSuffix = System.currentTimeMillis() + "_"
				+ String.valueOf(Math.abs(random.nextInt(10000)));

		//multi thread subscribe same topic
		ExecutorService subExecutorService = Executors.newFixedThreadPool(10);
		for (int i = 0; i < 10; i++) {
			final int topicSuffix = i + 1000;
			subExecutorService.submit(new Runnable() {

				public void run() {
					StreamingConnection sc = null;
					final StreamingCounter counter = new StreamingCounter();
					try {
						sc = NatsStreaming.connect("tuya_streaming", "test12345_multisub_sub_" + topicSuffix,
								builder.build());
						sc.subscribe("streaming/" + pubTopicSuffix, new MessageHandler() {

							public void onMessage(Message msg) {
								counter.increment();
								if (counter.value() % 10 == 0) {
									System.out.println("received: topic=streaming/" + pubTopicSuffix
											+ ",received count=" + counter.value() + ",clientId="
											+ "test12345_multisub_sub_" + topicSuffix);
								}
							}
						});
						msgSizeLatch.await();
						Thread.sleep(1000 * 60);//publish end sleep for receive remaining message
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

		//single thread publish
		ExecutorService pubExecutorService = Executors.newFixedThreadPool(1);
		final StringBuilder message = new StringBuilder();
		for (int i = 0; i < 256; i++) {
			message.append("i");
		}
		final byte[] msgByte = message.toString().getBytes();
		pubExecutorService.submit(new Runnable() {

			public void run() {
				StreamingConnection sc = null;
				try {
					sc = NatsStreaming.connect("tuya_streaming", "test12345_multisub_pub_" + pubTopicSuffix,
							builder.build());
					long beginTime = System.currentTimeMillis();
					for (int j = 1; j <= messageSize; j++) {
						sc.publish("streaming/" + pubTopicSuffix, msgByte);
						msgSizeLatch.countDown();
					}
					System.out.println(" send spend=" + (System.currentTimeMillis() - beginTime));
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
