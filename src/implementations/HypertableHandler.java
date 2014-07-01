package implementations;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.ClientException;

public class HypertableHandler {
	public static ThriftClient CLIENT;
	public static long NAMESPACE;
	
	public static void connectToDatabase(String host, String port){
		try {
			CLIENT = ThriftClient.create(host, Integer.parseInt(port));
			NAMESPACE = CLIENT.namespace_open("test");
		}
		catch (TTransportException e) {
			e.printStackTrace();
		}
		catch (TException e) {
			e.printStackTrace();
		}
		catch (ClientException e) {
			e.printStackTrace();
		}
	}

}
