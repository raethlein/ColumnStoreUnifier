package middleware;

import implementations.CassandraQueryHandler;
import implementations.DynamoDbQueryHandler;
import implementations.HBaseQueryHandler;
import implementations.HypertableQueryHandler;
import interfaces.MiddlewareInterface;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import model.Filter;
import model.Key;
import model.Row;

import com.amazonaws.services.dynamodbv2.model.ListTablesResult;


/**
 * {@inheritDoc}
 * 
 * @author Benjamin Räthlein
 * 
 */
public class QueryHandler implements MiddlewareInterface {

public void alterTableAddColumn(String tableName, String columnName) {
		switch (NoSQLMiddleware.getUsedDatabase()) {
		case Cassandra:
			CassandraQueryHandler.alterTableAddColumn(tableName, columnName);
			break;
		case DynamoDb:
			break;
		case Hbase:
			HBaseQueryHandler.alterTableAddColumnFamily(tableName, columnName);
			break;
		case Hypertable:
			HypertableQueryHandler.alterTableAddColumn(tableName, columnName);
			break;
		}
	}
	
	@Override
	public void createNamespace(String namespaceName){
		switch (NoSQLMiddleware.getUsedDatabase()) {
		case Cassandra:
			CassandraQueryHandler.createKeyspace(namespaceName);
			break;
		case DynamoDb:
			break;
		case Hbase:
			// Not supported in 0.94.21 version
			//HBaseQueryHandler.createNamespace(namespaceName);
			break;
		case Hypertable:
			HypertableQueryHandler.createNamespace(namespaceName);
			break;
		}
	}

	@Override
	public void createTable(String tableName, String primaryKey) {
		switch (NoSQLMiddleware.getUsedDatabase()) {
		case Cassandra:
			CassandraQueryHandler.createTable(tableName, primaryKey);
			break;
		case DynamoDb:
			DynamoDbQueryHandler.createTable(tableName, primaryKey);
			break;
		case Hbase:
			HBaseQueryHandler.createTable(tableName, primaryKey);
			break;
		case Hypertable:
			HypertableQueryHandler.createTable(tableName, primaryKey);
			break;
		}
	}

	@Override
	public List<String> getTableNames() {
		switch (NoSQLMiddleware.getUsedDatabase()) {
		case Cassandra:
			return CassandraQueryHandler.getTableNames();
		case DynamoDb:
			ListTablesResult result = DynamoDbQueryHandler.listTables();
			return result.getTableNames();
		case Hbase:
			return HBaseQueryHandler.getTableNames();
		case Hypertable:
			return HypertableQueryHandler.listTables();
		}
		return null;
	}

	@Override
	public void deleteTable(String tableName) {
		try {
			switch (NoSQLMiddleware.getUsedDatabase()) {
			case Cassandra:
				CassandraQueryHandler.deleteTable(tableName);
				break;
			case DynamoDb:
				DynamoDbQueryHandler.deleteTable(tableName);
				break;
			case Hbase:
				HBaseQueryHandler.deleteTable(tableName);
				break;
			case Hypertable:
				HypertableQueryHandler.deleteTable(tableName);
				break;
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void insertRows(String tableName, List<Row> items) {
		switch (NoSQLMiddleware.getUsedDatabase()) {
		case Cassandra:
			CassandraQueryHandler.insertItems(tableName, items);
			break;
		case DynamoDb:
			DynamoDbQueryHandler.insertItems(tableName, items);
			break;
		case Hbase:
			HBaseQueryHandler.insertItems(tableName, items);
			break;
		case Hypertable:
			HypertableQueryHandler.insertItems(tableName, items);
			break;
		}
	}

	@Override
	public Row getRowByKey(String tableName, Key... combinedKey) {
		switch (NoSQLMiddleware.getUsedDatabase()) {
		case Cassandra:
			return CassandraQueryHandler.getRowByKey(tableName, combinedKey);
		case DynamoDb:
			return DynamoDbQueryHandler.getRowByKey(tableName, combinedKey);
		case Hbase:
			return HBaseQueryHandler.getRowByKey(tableName, combinedKey);
		case Hypertable:
			return HypertableQueryHandler.getRowByKey(tableName, combinedKey[0].getValue());
		}

		return null;
	}

	//TODO: allow select only from one table and not set of tables
	@Override
	public List<Row> getRowsByKeys(Map<String, ArrayList<Map<String, String>>> tableNamesWithKeys) {
		switch (NoSQLMiddleware.getUsedDatabase()) {
		case Cassandra:
			break;
		case DynamoDb:
			return DynamoDbQueryHandler.getItemsByKeys(tableNamesWithKeys);
		case Hbase:
			//return HypertableQueryHandler.getRowsByKeys(tableNamesWithKeys.get(0));
		case Hypertable:
			break;
		}
		return null;
	}

	@Override
	public List<Row> getRows(String tableName, String conditionalOperator, Filter... filters) {
		switch (NoSQLMiddleware.getUsedDatabase()) {
		case Cassandra:
			return CassandraQueryHandler.scanTable(tableName, conditionalOperator, filters);
		case DynamoDb:
			return DynamoDbQueryHandler.scanTable(tableName, filters, conditionalOperator);
		case Hbase:
			return HBaseQueryHandler.scanTable(tableName, filters, conditionalOperator);
		case Hypertable:
			return HypertableQueryHandler.scanTable(tableName, conditionalOperator, filters);
		}

		return null;
	}
}
