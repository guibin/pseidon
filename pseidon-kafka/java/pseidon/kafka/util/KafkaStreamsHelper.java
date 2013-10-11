package pseidon.kafka.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public final class KafkaStreamsHelper {
	private static final Logger LOG = Logger.getLogger(KafkaStreamsHelper.class);
	
	private static ExecutorService service = Executors.newCachedThreadPool();

	/**
	 * Gets a Map of KafkaStreams and flattens them out into a single List
	 * 
	 * @param conn
	 * @param topicMap
	 * @return List of KafkaStream byte[] byte[]
	 */
	public static final BlockingQueue<MessageAndMetadata<byte[], byte[]>> get_streams(ConsumerConnector conn,
			Map<String, Integer> topicMap, int queue_limit) {
		final ArrayBlockingQueue<MessageAndMetadata<byte[], byte[]>> queue = new ArrayBlockingQueue<MessageAndMetadata<byte[], byte[]>>(queue_limit);
		
		List<KafkaStream<byte[], byte[]>> list = flatten(conn, topicMap);
		
		for (int i = 0; i < list.size();  i++) {
			final KafkaStream<byte[], byte[]> stream = list.get(0);
			service.submit(new Runnable() {
				public void run() {
					while (!Thread.interrupted()) {
						try {
							ConsumerIterator<byte[], byte[]> it = stream.iterator();
							while(it.hasNext()){
								MessageAndMetadata<byte[], byte[]> obj = it.next();
								queue.put(obj);
							}
						} catch (Throwable t) {
							LOG.error(t);
						}
					}
				}
			});
		}

		return queue;
	}

	private static final List<KafkaStream<byte[], byte[]>> flatten(
			ConsumerConnector conn, Map<String, Integer> topicMap) {
		Map<String, List<KafkaStream<byte[], byte[]>>> map = conn
				.createMessageStreams(topicMap);
		// we do this is java because for some reason the java scala bindings to
		// not work well here with clojure
		List<KafkaStream<byte[], byte[]>> streams = new ArrayList<KafkaStream<byte[], byte[]>>();
		for (List<KafkaStream<byte[], byte[]>> listStreams : map.values())
			for (KafkaStream<byte[], byte[]> stream : listStreams)
				streams.add(stream);
		
		
		return streams;
	}

}
