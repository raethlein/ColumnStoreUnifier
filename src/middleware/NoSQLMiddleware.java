package middleware;
import implementations.CassandraHandler;
import implementations.DynamoDbHandler;
import implementations.HBaseHandler;
import implementations.HypertableHandler;
import interfaces.MiddlewareInterface;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


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
	
	private static MiddlewareInterface queryHandler;

	public synchronized static MiddlewareInterface getQueryHandler() {
		if (queryHandler == null) {
			init();
			ComparisonOperatorMapper.initConditionalOperatorMapper();
			
			switch (getDatabase()) {
			case "DynamoDb":
				setUsedDatabase(Implementations.DynamoDb);
				DynamoDbHandler.connectToDatabase(databaseHost + ":" + databasePort);
				queryHandler = new DynamoDbHandler();
				break;
			case "Hbase":
				setUsedDatabase(Implementations.Hbase);
				HBaseHandler.connect();
				queryHandler = new HBaseHandler();
				break;
			case "Hypertable":
				setUsedDatabase(Implementations.Hypertable);
				queryHandler = new HypertableHandler();
				queryHandler.connectToDatabase(databaseHost, databasePort);
				break;
			case "Cassandra":
				setUsedDatabase(Implementations.Cassandra);
				queryHandler = new CassandraHandler();
				queryHandler.connectToDatabase(databaseHost, databasePort);
				break;
			}
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
