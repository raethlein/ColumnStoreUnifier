package implementations;

import interfaces.MiddlewareInterface;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import middleware.ConnectionHandler;
import model.Filter;
import model.Key;
import model.Row;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;


public class CassandraHandler implements MiddlewareInterface, ConnectionHandler {

	static String name = "Cassandra";
	static Cluster cluster;
	static Session session;
	static String keyspace = "test";
	
	public static void connectToDatabase(String address) {
		cluster = Cluster.builder().addContactPoint(address).build();
		session = cluster.connect(keyspace);
	}
	
	@Override
	public void connectToDatabase(String host, String port) {
		cluster = Cluster.builder().addContactPoint(host).build();
		session = cluster.connect(keyspace);		
	}

	@Override
	public void alterTableAddColumn(String tableName, String columnName) {
		CassandraQueryHandler.alterTableAddColumn(tableName, columnName);
	}

	@Override
	public void createNamespace(String namespaceName) {
		CassandraQueryHandler.createKeyspace(namespaceName);
	}

	@Override
	public void createTable(String tableName, String primaryKey) {
		CassandraQueryHandler.createTable(tableName, primaryKey);
	}

	@Override
	public void deleteTable(String tableName) {
		CassandraQueryHandler.deleteTable(tableName);
	}

	@Override
	public void insertRows(String tableName, List<Row> rows) {
		CassandraQueryHandler.insertItems(tableName, rows);
	}

	@Override
	public Row getRowByKey(String tableName, Key... combinedKey) {
		return CassandraQueryHandler.getRowByKey(tableName, combinedKey);
	}

	@Override
	public List<Row> getRowsByKeys(
			Map<String, ArrayList<Map<String, String>>> tableNamesWithKeys) {
		return null;
	}

	@Override
	public List<Row> getRows(String tableName, String conditionalOperator,
			Filter... filters) {
		return CassandraQueryHandler.scanTable(tableName, conditionalOperator, filters);
	}

	@Override
	public List<String> getTableNames() {
		return CassandraQueryHandler.getTableNames();
	}
}
