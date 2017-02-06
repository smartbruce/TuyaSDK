
package com.tuya.smart;

import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;

public class StreamingPubTest {

	public static void main(String[] args) throws Exception {
		Options.Builder builder = new Options.Builder();
		builder.natsUrl("nats://118.89.149.174:51205");
		StreamingConnection sc = NatsStreaming.connect("tuya_streaming", "test12345_pub", builder.build());
		StringBuilder message = new StringBuilder();
		for (int i = 0; i <= 20; i++) {//1M
			message.append("i");
		}
		try {
			long beginTime = System.currentTimeMillis();
			for (int i = 500000; i > 0; i--) {
				sc.publish("foo", message.toString().getBytes());
			}
			System.out.println("send spend=" + (System.currentTimeMillis() - beginTime));
		} finally {
			sc.close();
		}
	}

}
