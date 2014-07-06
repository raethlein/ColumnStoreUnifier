package implementations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import model.Attribute;
import model.Filter;
import model.Key;
import model.Row;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public class DynamoDbQueryHandler {

	// Provide initial provisioned throughput values as Java long data types
	private static ProvisionedThroughput DEFAULT_PROVISIONED_THROUGHPUT = new ProvisionedThroughput()
			.withReadCapacityUnits(5L).withWriteCapacityUnits(5L);

	/**
	 * Create a table with the given hashKey as row id
	 * 
	 * @param tableName
	 * @param primaryKey
	 */
	public static void createTable(String tableName, String primaryKey) {
		ArrayList<KeySchemaElement> ks = new ArrayList<KeySchemaElement>();
		ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();

		ks.add(new KeySchemaElement().withAttributeName(primaryKey)
				.withKeyType(KeyType.HASH));
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				primaryKey).withAttributeType("S"));

		CreateTableRequest request = new CreateTableRequest()
				.withTableName(tableName).withKeySchema(ks)
				.withProvisionedThroughput(DEFAULT_PROVISIONED_THROUGHPUT);

		request.setAttributeDefinitions(attributeDefinitions);
		try {
			DynamoDbHandler.CLIENT.createTable(request);
		} catch (ResourceInUseException e) {
			//System.err.println("Table '" + tableName + "' already exists");
		}
	}

	public static void createTable(String tableName, long readCapacityUnits,
			long writeCapacityUnits, String hashKeyName, String hashKeyType,
			String rangeKeyName, String rangeKeyType) {
	}

	public static ListTablesResult listTables() {
		return DynamoDbHandler.CLIENT.listTables();
	}

	/**
	 * Delete the table with given name
	 * 
	 * @param tableName
	 */
	public static void deleteTable(String tableName) {
		try {
			DynamoDbHandler.CLIENT
					.deleteTable(new DeleteTableRequest(tableName));
		} catch (Exception e) {
			//Table does not exist
		}
	}

	/**
	 * Insert values into the given table. Items are stored as blobs in the
	 * database.
	 * 
	 * @param tableName
	 * @param items
	 */
	public static void insertItems(String tableName, List<Row> items) {
		Map<String, AttributeValue> transformedItem = new HashMap<>();
		for (Row item : items) {
			for (Attribute attribute : item.getAttributes()) {
				transformedItem.put(attribute.getName(),
						new AttributeValue().withS(attribute.getValue()));
			}
			transformedItem.put(item.getKey().getName(),
					new AttributeValue().withS(item.getKey().getValue()));
			PutItemRequest itemRequest = new PutItemRequest().withTableName(
					tableName).withItem(transformedItem);
			DynamoDbHandler.CLIENT.putItem(itemRequest);
			transformedItem.clear();
		}

		//
		// for (Map<String, String> item : items) {
		// for (String key : item.keySet()) {
		// transformedItem.put(key, new
		// AttributeValue().withB(ByteBuffer.wrap(item.get(key).getBytes())));
		// }
		// PutItemRequest itemRequest = new
		// PutItemRequest().withTableName(tableName).withItem(transformedItem);
		// DynamoDbHandler.CLIENT.putItem(itemRequest);
		// transformedItem.clear();
		// }
	}

	/**
	 * Get item of table with provided key-value.
	 * 
	 * @param tableName
	 * @param combinedKey
	 * @return
	 */
	public static Row getRowByKey(String tableName, Key... combinedKey) {
		Map<String, AttributeValue> transformedKey = new HashMap<>();
		for (Key key : combinedKey) {
			transformedKey.put(key.getName(),
					new AttributeValue().withS(key.getValue()));
		}

		GetItemResult result = DynamoDbHandler.CLIENT
				.getItem(new GetItemRequest(tableName, transformedKey));

		List<Attribute> attributes = new ArrayList<>();
		for (String resultKey : result.getItem().keySet()) {
			attributes.add(new Attribute(resultKey, result.getItem()
					.get(resultKey).getS()));
		}

		return new Row(attributes);
	}

	/**
	 * Get items from different tables by values of their key.
	 * 
	 * @param tableName
	 * @param combinedKey
	 * @return
	 */
	@Deprecated
	public static List<Row> getItemsByKeys(
			Map<String, ArrayList<Map<String, String>>> tableNamesWithKeys) {
		HashMap<String, KeysAndAttributes> requestItems = new HashMap<String, KeysAndAttributes>();

		for (String tableName : tableNamesWithKeys.keySet()) {
			ArrayList<Map<String, String>> keyList = tableNamesWithKeys
					.get(tableName);

			ArrayList<Map<String, AttributeValue>> transformedKeyList = new ArrayList<>();
			for (Map<String, String> keyValueMap : keyList) {
				Map<String, AttributeValue> transformedItem = new HashMap<>();
				for (String key : keyValueMap.keySet()) {
					transformedItem.put(key,
							new AttributeValue().withS(keyValueMap.get(key)));
				}
				transformedKeyList.add(transformedItem);
			}
			requestItems.put(tableName,
					new KeysAndAttributes().withKeys(transformedKeyList));
		}

		BatchGetItemResult result = DynamoDbHandler.CLIENT
				.batchGetItem(new BatchGetItemRequest()
						.withRequestItems(requestItems));

		List<Row> items = new ArrayList<>();
		for (String tableName : tableNamesWithKeys.keySet()) {
			List<Map<String, AttributeValue>> tableResults = result
					.getResponses().get(tableName);
			List<Attribute> attributes = null;

			for (Map<String, AttributeValue> tableResult : tableResults) {
				attributes = new ArrayList<>();
				for (String key : tableResult.keySet()) {
					attributes.add(new Attribute(key, tableResult.get(key)
							.getS()));
				}
				items.add(new Row(attributes));
			}
		}
		return items;
	}

	/**
	 * Returns the the result of the scanned table with the applied filters. The
	 * filters are connected via the conditionalOperator. It can either be AND
	 * or OR.
	 * 
	 * @param tableName
	 *            The table to be scanned.
	 * @param filters
	 *            A list with Filter objects.
	 * @param conditionalOperator
	 *            AND | OR - Conditional clauses in DynamoDb can either have AND
	 *            or OR connectors, no mix.
	 * @return
	 */
	public static List<Row> scanTable(String tableName, Filter[] filters,
			String conditionalOperator) {
		Map<String, Condition> scanFilter = new HashMap<>();

		for (Filter filter : filters) {
			scanFilter.put(
					filter.getAttribute().getName(),
					new Condition().withComparisonOperator(
							filter.getComparisonOperator())
							.withAttributeValueList(
									new AttributeValue().withS(filter
											.getAttribute().getValue())));
		}

		ScanResult scanResult;

		if (filters.length == 1) {
			scanResult = DynamoDbHandler.CLIENT.scan(new ScanRequest(tableName)
					.withScanFilter(scanFilter));
		} else {
			scanResult = DynamoDbHandler.CLIENT.scan(new ScanRequest(tableName)
					.withConditionalOperator(conditionalOperator)
					.withScanFilter(scanFilter));
		}

		ArrayList<Row> items = new ArrayList<>();
		List<Attribute> attributes = null;
		for (Map<String, AttributeValue> result : scanResult.getItems()) {
			attributes = new ArrayList<>();
			for (String key : result.keySet()) {
				attributes.add(new Attribute(key, result.get(key).getS()));
			}
			items.add(new Row(attributes));
		}

		return items;
	}
}
