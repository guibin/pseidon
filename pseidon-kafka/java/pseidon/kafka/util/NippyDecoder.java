package pseidon.kafka.util;

import clojure.lang.RT;
import clojure.lang.Var;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

public class NippyDecoder implements Decoder{

	Var thaw = RT.var("taoensso.nippy", "thaw");
	
	public NippyDecoder(VerifiableProperties props){
		
	}
	
	public Object fromBytes(byte[] bts){
		return thaw.invoke(bts);
	}
	
}
