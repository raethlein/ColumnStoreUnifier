package middleware;
import implementations.DynamoDbHandler;
import implementations.HypertableHandler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class Configurator {

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

	public static void init() {
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
			case "Hypertable":
				setUsedDatabase(Implementations.Hypertable);
				HypertableHandler.connectToDatabase(databaseHost, databasePort);
				break;
			case "Cassandra":
				setUsedDatabase(Implementations.Cassandra);
				//CassandraHandler.connectToDatabase(databaseHost);
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
		Configurator.database = database;
	}

	public static Implementations getUsedDatabase() {
		return usedDatabase;
	}

	public static void setUsedDatabase(Implementations usedDatabase) {
		Configurator.usedDatabase = usedDatabase;
	}

}
