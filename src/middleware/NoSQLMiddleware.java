package middleware;
import implementations.CassandraHandler;
import implementations.DynamoDbHandler;
import implementations.HBaseHandler;
import implementations.HypertableHandler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.hbase.thrift.HbaseHandlerMetricsProxy;
import org.apache.hadoop.hbase.thrift.generated.Hbase;


public class NoSQLMiddleware {

	private static String database;
	private static String databaseHost;
	private static String databasePort;
	private static Implementations usedDatabase;


	public enum Implementations {
		Cassandra,
		DynamoDb,
		Hbase,
		Hypertable
	}
	
	private static QueryHandler queryHandler;

	public synchronized static QueryHandler getQueryHandler() {
		if (queryHandler == null) {
			init();
			ComparisonOperatorMapper.initConditionalOperatorMapper();
			queryHandler = new QueryHandler();
		}
		
		return queryHandler;
	}

	private static void init() {
		InputStream input = null;
		Properties properties = new Properties();

		try {
			input = new FileInputStream("./res/config.properties");
			properties.load(input);

			setDatabase(properties.getProperty("database"));
			databaseHost = properties.getProperty("databaseHost");
			databasePort = properties.getProperty("databasePort");

			switch (getDatabase()) {
			case "DynamoDb":
				setUsedDatabase(Implementations.DynamoDb);
				DynamoDbHandler.connectToDatabase(databaseHost + ":" + databasePort);
				break;
			case "Hbase":
				setUsedDatabase(Implementations.Hbase);
				HBaseHandler.connect();
				break;
			case "Hypertable":
				setUsedDatabase(Implementations.Hypertable);
				HypertableHandler.connectToDatabase(databaseHost, databasePort);
				break;
			case "Cassandra":
				setUsedDatabase(Implementations.Cassandra);
				CassandraHandler.connectToDatabase(databaseHost);
				break;
			}

		}
		catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	public static String getDatabase() {
		return database;
	}

	public static void setDatabase(String database) {
		NoSQLMiddleware.database = database;
	}

	public static Implementations getUsedDatabase() {
		return usedDatabase;
	}

	public static void setUsedDatabase(Implementations usedDatabase) {
		NoSQLMiddleware.usedDatabase = usedDatabase;
	}

}
