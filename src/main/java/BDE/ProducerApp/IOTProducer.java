package BDE.ProducerApp;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.LinkedList;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * Hello world!
 *
 */
public class IOTProducer {
	static Random sensorRandom;

	private static LinkedList<UUID> sensorIDs;

	public final static String TOPIC = "iottopic";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";
	private final static String CLIENTID = "IOTProducer";

	private static String selectSensor() {
		if (sensorRandom == null) {
			sensorRandom = new Random(System.currentTimeMillis());
		}
		if (sensorIDs == null) {
			sensorIDs = new LinkedList<UUID>();
			for (int i = 0; i < 10; i++)
				sensorIDs.add(UUID.randomUUID());
		}
		return sensorIDs.get(sensorRandom.nextInt(sensorIDs.size() - 1)).toString();
	}

	private static String getSensorValue() {
		String sensorValue = selectSensor() + "," + sensorRandom.nextDouble() * sensorRandom.nextInt(10);

		return sensorValue;
	}

	private static Producer<Long, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENTID);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
				LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}

	public static void main(String[] args) {
		int sendMessageCount = 10;

		if (args.length == 1) {
			try {
				sendMessageCount = Integer.parseInt(args[0]);
			} catch (NumberFormatException e) {
				// System.err.println("Argument" + args[0] + " must be an
				// integer.");
			}
		}

		final Producer<Long, String> producer = createProducer();
		long time = System.currentTimeMillis();

		try {
			System.out.println("sending...");
			for (long index = time; index < time + sendMessageCount; index++) {

				final ProducerRecord<Long, String> record = 
						new ProducerRecord<>(TOPIC, index, getSensorValue());
				producer.send(record);
			}
			System.out.println("done.");
		} finally {
			producer.flush();
			producer.close();
		}

	}
}
