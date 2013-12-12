package pseidon.kafka.util;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class DefaultEncoder implements Encoder{

	public DefaultEncoder(VerifiableProperties props){
		
	}
	
	public byte[] toBytes(Object o) {
		if(o == null)
			return new byte[0];
		else if(o instanceof byte[])
			return (byte[])o;
		else{
			try{
				return o.toString().getBytes();
			}catch(Throwable t){
				return new byte[0];
			}
		}
	}
}
