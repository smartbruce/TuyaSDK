
package com.tuya.smart;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;

public class MultiPubMultiSubStreamingTest {

	public static void main(String[] args) throws Exception {
		final int messageSize = 10000;
		final CountDownLatch msgSizeLatch = new CountDownLatch(messageSize);
		int threadSize = StreamingConstants.DEFAULT_THREAD_SIZE;
		final Options.Builder builder = new Options.Builder();
		builder.natsUrl("nats://118.89.149.174:51205");

		//multi thread subscribe
		ExecutorService subExecutorService = Executors.newFixedThreadPool(threadSize);
		for (int i = 0; i < threadSize; i++) {
			final int topicSuffix = i + 1000;
			subExecutorService.submit(new Runnable() {

				public void run() {
					StreamingConnection sc = null;
					final StreamingCounter counter = new StreamingCounter();
					try {
						sc = NatsStreaming.connect("tuya_streaming", "test12345_multipubsub_sub_" + topicSuffix,
								builder.build());
						sc.subscribe("streaming/" + topicSuffix, new MessageHandler() {

							public void onMessage(Message msg) {
								counter.increment();
								if (counter.value() % 10 == 0) {
									System.out.println("received: topic=streaming/" + topicSuffix + ",received count="
											+ counter.value() + ",clientId=" + "test12345_multipubsub_sub_"
											+ topicSuffix);
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

		//multi thread publish
		ExecutorService pubExecutorService = Executors.newFixedThreadPool(threadSize);
		final StringBuilder message = new StringBuilder();
		for (int i = 0; i < 256; i++) {
			message.append("i");
		}
		final byte[] msgByte = message.toString().getBytes();
		for (int i = 0; i < threadSize; i++) {
			final int topicSuffix = i + 1000;
			pubExecutorService.submit(new Runnable() {

				public void run() {
					StreamingConnection sc = null;
					try {
						sc = NatsStreaming.connect("tuya_streaming", "test12345_multipubsub_pub_" + topicSuffix,
								builder.build());
						long beginTime = System.currentTimeMillis();
						for (int j = 1; j <= messageSize; j++) {
							sc.publish("streaming/" + topicSuffix, msgByte);
							msgSizeLatch.countDown();
						}
						System.out.println(topicSuffix + " send spend=" + (System.currentTimeMillis() - beginTime));
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
}
