package interfaces;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import model.Filter;
import model.Key;
import model.Row;


public interface MiddlewareInterface {
	public void connectToDatabase(String databaseHost, String databasePort);
	
	public void alterTableAddColumn(String tableName, String columnName);
	
	public void createNamespace(String namespaceName);
	
	/**
	 * Create a table with given name and indexed column.
	 * @param tableName
	 * @param primaryKey DynamoDb: indexed primary key; Hypertable: indexed column family
	 */
	public void createTable(String tableName, String primaryKey);
	public void deleteTable(String tableName);
	/**
	 * Insert rows into the table with given name.
	 * @param tableName
	 * @param rows
	 */
	public void insertRows(String tableName, List<Row> rows);
	public Row getRowByKey(String tableName, Key... combinedKey);
	@Deprecated
	public List<Row> getRowsByKeys(Map<String, ArrayList<Map<String, String>>> tableNamesWithKeys);
	public List<Row> getRows(String tableName, String conditionalOperator, Filter... filters);
	public List<String> getTableNames();
}
