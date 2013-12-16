package pseidon.kafka.util;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import clojure.lang.RT;
import clojure.lang.Var;

public class NippyEncoder implements Encoder{

	Var freeze = RT.var("taoensso.nippy", "freeze");
	
	public NippyEncoder(VerifiableProperties props){
		
	}

	public byte[] toBytes(Object obj){
		return (byte[])freeze.invoke(obj);
	}
}
