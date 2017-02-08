
package com.tuya.smart;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;

public class StreamingPubTest {

	public static void main(String[] args) throws Exception {
		final StringBuilder message = new StringBuilder();
		for (int i = 0; i < 256; i++) {
			message.append("i");
		}
		final byte[] msgByte = message.toString().getBytes();
		int threadSize = StreamingConstants.DEFAULT_THREAD_SIZE;
		ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
		for (int i = 0; i < threadSize; i++) {
			final int topicSuffix = i + 1;
			executorService.submit(new Runnable() {

				public void run() {
					StreamingConnection sc = null;
					try {
						Options.Builder builder = new Options.Builder();
						builder.natsUrl("nats://118.89.149.174:51205");
						sc = NatsStreaming.connect("tuya_streaming", "test12345_pub_" + topicSuffix, builder.build());
						long beginTime = System.currentTimeMillis();
						for (int j = 10000; j > 0; j--) {
							sc.publish("streaming/" + topicSuffix, msgByte);
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
