package implementations;
import interfaces.MiddlewareInterface;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import model.Filter;
import model.Key;
import model.Row;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;


public class DynamoDbHandler implements MiddlewareInterface {
	static AmazonDynamoDBClient CLIENT = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
	static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	
	public static void connectToDatabase(String address){
		CLIENT.setEndpoint(address);
	}

	@Override
	public void alterTableAddColumn(String tableName, String columnName) {
		//TODO
	}

	@Override
	public void createNamespace(String namespaceName) {
		//TODO
	}

	@Override
	public void createTable(String tableName, String primaryKey) {
		DynamoDbQueryHandler.createTable(tableName, primaryKey);
	}

	@Override
	public void deleteTable(String tableName) {
		DynamoDbQueryHandler.deleteTable(tableName);
	}

	@Override
	public void insertRows(String tableName, List<Row> rows) {
		DynamoDbQueryHandler.insertItems(tableName, rows);
	}

	@Override
	public Row getRowByKey(String tableName, Key... combinedKey) {
		return DynamoDbQueryHandler.getRowByKey(tableName, combinedKey);
	}

	@Override
	public List<Row> getRowsByKeys(
			Map<String, ArrayList<Map<String, String>>> tableNamesWithKeys) {
		return DynamoDbQueryHandler.getItemsByKeys(tableNamesWithKeys);
	}

	@Override
	public List<Row> getRows(String tableName, String conditionalOperator,
			Filter... filters) {
		return DynamoDbQueryHandler.scanTable(tableName, filters, conditionalOperator);
	}

	@Override
	public List<String> getTableNames() {
		ListTablesResult result = DynamoDbQueryHandler.listTables();
		return result.getTableNames();
	}
}
