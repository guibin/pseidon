package pseidon.kafka.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.log4j.Logger;

public final class KafkaStreamsHelper {
	private static final Logger LOG = Logger
			.getLogger(KafkaStreamsHelper.class);

	private static final ExecutorService service = Executors
			.newCachedThreadPool();

	/**
	 * Gets a Map of KafkaStreams and flattens them out into a single List
	 * 
	 * @param conn
	 * @param topicMap
	 * @return List of KafkaStream byte[] byte[]
	 */
	public static final BlockingQueue<MessageAndMetadata<byte[], byte[]>> get_streams(
			ConsumerConnector conn, Map<String, Integer> topicMap,
			int queue_limit) {
		final ArrayBlockingQueue<MessageAndMetadata<byte[], byte[]>> queue = new ArrayBlockingQueue<MessageAndMetadata<byte[], byte[]>>(
				queue_limit);

		List<KafkaStream<byte[], byte[]>> list = flatten(conn, topicMap);

		for (int i = 0; i < list.size(); i++) {
			final int index = i;
			final KafkaStream<byte[], byte[]> stream = list.get(i);
			LOG.info("!!!!!!!!!!!!!!!!!!!!!!  submit stream " + i);
			service.submit(new Runnable() {
				public void run() {
					LOG.info("consuming from iterator " + index);
					while (!Thread.interrupted()) {
						try {
							LOG.info("consuming from iterator2 " + index);
							final ConsumerIterator<byte[], byte[]> it = stream
									.iterator();

							while (!Thread.interrupted()) {
								try {
									final MessageAndMetadata<byte[], byte[]> obj = it
											.next();
									queue.put(obj);
								} catch (java.util.NoSuchElementException ne) {
									Thread.sleep(500);
								}
							}
						} catch (Throwable t) {
							LOG.error(t.toString(), t);
						}
						
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							LOG.info("Thread interrupted, exit");
							Thread.currentThread().interrupt();
							return;
						}
						
					}
				}
			});
		}

		return queue;
	}


	public static final List<KafkaStream<byte[], byte[]>> flatten(
			ConsumerConnector conn, Map<String, Integer> topicMap) {
		try {
			LOG.info("!!!!!!!!!!!!!!!!!!!!!! Get streams from topicMap " + topicMap);
			Map<String, List<KafkaStream<byte[], byte[]>>> map = conn
					.createMessageStreams(topicMap);
			LOG.info("!!!!!!!!!!!!!!!!!!!!!! got streams");
			// we do this is java because for some reason the java scala
			// bindings to
			// not work well here with clojure
			List<KafkaStream<byte[], byte[]>> streams = new ArrayList<KafkaStream<byte[], byte[]>>();
			for (List<KafkaStream<byte[], byte[]>> listStreams : map.values())
				for (KafkaStream<byte[], byte[]> stream : listStreams)
					streams.add(stream);
			LOG.info("returning streams !!!!!!!!!!!!!!!!!!!!!! ");
			return streams;
		} catch (org.I0Itec.zkclient.exception.ZkNodeExistsException e) {
			RuntimeException rte = new RuntimeException(
					"Kafka does not allow calling createMesasgeStreams for the same connector multiple times",
					e);
			rte.setStackTrace(e.getStackTrace());
			throw rte;
		}
	}

	public static final void main(String args[]) throws InterruptedException {
		Properties props = new Properties();
		props.setProperty(
				"zookeeper.connect",
				"hbnn1.dw.sc.gwallet.com,hbnn2.dw.sc.gwallet.com,hb01.dw.sc.gwallet.com/pseidon/kafka/8/1");
		// props.setProperty("zookeeper.connect", "localhost");
		props.setProperty("metadata.broker.list", "hb03:9092");
		props.setProperty("group.id", args[1]);
		props.setProperty("auto.offset.reset", "smallest");
		props.setProperty("client.id", "pseidon");

		ConsumerConfig conf = new ConsumerConfig(props);

		ConsumerConnector conn = Consumer.createJavaConsumerConnector(conf);
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		System.out.println("topic: " + args[0]);
		topicMap.put(args[0], 1);

		BlockingQueue<MessageAndMetadata<byte[], byte[]>> queue = get_streams(
				conn, topicMap, 10);
		Thread.sleep(2000);

		while (!Thread.interrupted())
			System.out.println("msg: " + queue.take());

	}

}
