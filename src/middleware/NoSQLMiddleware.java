package middleware;
import implementations.CassandraHandler;
import implementations.DynamoDbHandler;
import implementations.HBaseHandler;
import implementations.HypertableHandler;
import interfaces.MiddlewareInterface;


public class NoSQLMiddleware {

	private static Implementations usedDatabase;

	public enum Implementations {
		Cassandra,
		DynamoDb,
		Hbase,
		Hypertable
	}
	
	private static MiddlewareInterface queryHandler;

	public synchronized static MiddlewareInterface getQueryHandler(
			Implementations database, String databaseHost, String databasePort) {		
		if (queryHandler == null) {
			ComparisonOperatorMapper.initConditionalOperatorMapper();
			
			switch (database) {
			case DynamoDb:
				setUsedDatabase(Implementations.DynamoDb);
				DynamoDbHandler.connectToDatabase(databaseHost + ":" + databasePort);
				queryHandler = new DynamoDbHandler();
				break;
			case Hbase:
				setUsedDatabase(Implementations.Hbase);
				HBaseHandler.connect();
				queryHandler = new HBaseHandler();
				break;
			case Hypertable:
				setUsedDatabase(Implementations.Hypertable);
				queryHandler = new HypertableHandler();
				queryHandler.connectToDatabase(databaseHost, databasePort);
				break;
			case Cassandra:
				setUsedDatabase(Implementations.Cassandra);
				queryHandler = new CassandraHandler();
				queryHandler.connectToDatabase(databaseHost, databasePort);
				break;
			}
		}
		
		return queryHandler;
	}

	public static Implementations getUsedDatabase() {
		return usedDatabase;
	}

	public static void setUsedDatabase(Implementations usedDatabase) {
		NoSQLMiddleware.usedDatabase = usedDatabase;
	}

}
