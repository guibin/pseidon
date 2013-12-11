package pseidon.kafka.util;

import java.util.concurrent.atomic.AtomicInteger;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * 
 * Round robin partitioner using a simple thread safe AotmicInteger
 * 
 */
public class RoundRobinPartitioner implements Partitioner<Object>{
    AtomicInteger counter = new AtomicInteger(0);
    
    public RoundRobinPartitioner(VerifiableProperties props){
    	
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
