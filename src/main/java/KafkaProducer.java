import java.util.*;

import com.google.gson.Gson;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
	public static void main(String[] args) {
		long events = Long.parseLong("1000");
		Random rnd = new Random();
		VisitData visitData = null;
		Gson gson = new Gson();

		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// props.put("partitioner.class", "example.producer.SimplePartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);

		for (long nEvents = 0; nEvents < events; nEvents++) {
			visitData = new VisitData();
			long runtime = new Date().getTime();
			String ip = "192.168.2." + rnd.nextInt(255);
			//String msg = runtime + ",www.example.com," + ip;
			visitData.setIp(ip);
			visitData.setEventDate(new Date());
			visitData.setDomain("www.example.com");
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					"page_visits", gson.toJson(visitData));
			producer.send(data);
		}
		producer.close();
	}
}