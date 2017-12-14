package BDE.ProducerApp;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class IOTConsumer {

	public static class KafkaConsumerRunner implements Runnable {
		private final AtomicBoolean closed = new AtomicBoolean(false);
		private final KafkaConsumer<Long, String> consumer;

		public KafkaConsumerRunner(KafkaConsumer<Long, String> consumer) {
			this.consumer = consumer;
		}

		public void run() {
			try {
				consumer.subscribe(Arrays.asList(IOTProducer.TOPIC));
				while (!closed.get()) {
					ConsumerRecords<Long, String> records = consumer.poll(100);
					for (ConsumerRecord<Long, String> record : records)
						System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
								record.value());
				}
			} catch (WakeupException e) {
				// Ignore exception if closing
				if (!closed.get())
					throw e;
			} finally {
				consumer.close();
			}
		}

		// Shutdown hook which can be called from a separate thread
		public void shutdown() {
			closed.set(true);
			consumer.wakeup();
		}
	}

	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "IOTSample");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props);
		
		KafkaConsumerRunner r = new KafkaConsumerRunner(consumer);
		Thread t = new Thread(r);
		System.out.println("waiting for messages...");
		t.start();
		Thread.sleep(10000);
		r.shutdown();
		System.out.println("shutting down ...");
		t.join();
		System.out.println("done.");
	}
}
