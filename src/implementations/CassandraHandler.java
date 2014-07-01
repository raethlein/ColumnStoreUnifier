package implementations;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;


public class CassandraHandler {

	static Cluster cluster;
	static Session session;
	static String keyspace = "test";
	
	public static void connectToDatabase(String address) {
		cluster = Cluster.builder().addContactPoint(address).build();
		Metadata metadata = cluster.getMetadata();
		session = cluster.connect(keyspace);
	}
}
