package implementations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import interfaces.MiddlewareInterface;
import model.Filter;
import model.Key;
import model.Row;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseHandler implements MiddlewareInterface{
	static Configuration config;
	public static void connect() {
		config = HBaseConfiguration.create();
	}
	
	@Override
	public void connectToDatabase(String databaseHost, String databasePort) {		
	}

	@Override
	public void alterTableAddColumn(String tableName, String columnName) {
		HBaseQueryHandler.alterTableAddColumnFamily(tableName, columnName);
	}
	@Override
	public void createNamespace(String namespaceName) {
		HBaseQueryHandler.createNamespace(namespaceName);
	}
	@Override
	public void createTable(String tableName, String primaryKey) {
		HBaseQueryHandler.createTable(tableName, primaryKey);
	}
	@Override
	public void deleteTable(String tableName) {
		HBaseQueryHandler.deleteTable(tableName);
	}
	@Override
	public void insertRows(String tableName, List<Row> rows) {
		HBaseQueryHandler.insertItems(tableName, rows);
	}
	@Override
	public Row getRowByKey(String tableName, Key... combinedKey) {
		return HBaseQueryHandler.getRowByKey(tableName, combinedKey);
	}
	@Override
	public List<Row> getRowsByKeys(
			Map<String, ArrayList<Map<String, String>>> tableNamesWithKeys) {
		return null;
	}
	@Override
	public List<Row> getRows(String tableName, String conditionalOperator,
			Filter... filters) {
		return HBaseQueryHandler.scanTable(tableName, filters, conditionalOperator);
	}
	@Override
	public List<String> getTableNames() {
		return HBaseQueryHandler.getTableNames();
	}

}
