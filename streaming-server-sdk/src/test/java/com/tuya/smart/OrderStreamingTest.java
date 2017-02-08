
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

public class OrderStreamingTest {

	public static void main(String[] args) throws Exception {
		final Options.Builder builder = new Options.Builder();
		builder.natsUrl("nats://118.89.149.174:51205");
		ExecutorService executorService = Executors.newFixedThreadPool(2);
		final int messageSize = 10000;
		final CountDownLatch msgSizeLatch = new CountDownLatch(messageSize);

		Random random = new Random();
		final String topicSuffix = System.currentTimeMillis() + "_" + String.valueOf(Math.abs(random.nextInt(10000)));

		//subscribe thread
		executorService.submit(new Runnable() {

			public void run() {
				StreamingConnection sc = null;
				try {
					sc = NatsStreaming.connect("tuya_streaming", "test12345_sub_" + topicSuffix, builder.build());
					sc.subscribe("streaming/" + topicSuffix, new MessageHandler() {

						public void onMessage(Message msg) {
							System.out.println("received: sequence=" + msg.getSequence() + ",message="
									+ new String(msg.getData()));
						}
					});
					msgSizeLatch.await();
					Thread.sleep(1000 * 6);//publish end sleep for receive remaining message
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

		//publish thread
		executorService.submit(new Runnable() {

			public void run() {
				StreamingConnection sc = null;
				try {
					sc = NatsStreaming.connect("tuya_streaming", "test12345_pub_" + topicSuffix, builder.build());
					long beginTime = System.currentTimeMillis();
					for (int j = 1; j <= messageSize; j++) {
						String message = j + "_tuya_streaming_test_message_order";
						sc.publish("streaming/" + topicSuffix, message.getBytes());
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
