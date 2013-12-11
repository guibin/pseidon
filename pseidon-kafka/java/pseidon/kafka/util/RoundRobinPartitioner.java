package pseidon.kafka.util;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * 
 * Round robin partitioner using a simple thread safe AotmicInteger
 * 
 */
public class RoundRobinPartitioner implements Partitioner{
	private static final Logger log = Logger.getLogger(RoundRobinPartitioner.class);
	
    AtomicInteger counter = new AtomicInteger(0);
    
    public RoundRobinPartitioner(VerifiableProperties props){
    	  log.info("===========>>> Using Round Robin Partitioner");
    }
    
	public int partition(Object key, int partitions){
		int i = counter.getAndIncrement();
		if(i == Integer.MAX_VALUE){
			counter.set(0);
		    return 0;
		}else
		    return i % partitions;
	}
}
