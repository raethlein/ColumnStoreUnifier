import interfaces.MiddlewareInterface;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import middleware.NoSQLMiddleware;
import model.Attribute;
import model.Key;
import model.Row;

public class TestHandler {

	public static String TABLE_NAME = "test_mushroom_kingdom";
	public static String TABLE_NAME2 = "test_hyrule";
	public static String INDEX = "id";
	public static MiddlewareInterface queryHandler = NoSQLMiddleware
			.getQueryHandler();

	public static void insertTestItems(String tableName) {
		List<Row> items = new ArrayList<>();
		List<Attribute> attributes = new ArrayList<>();

		if (tableName.equals(TABLE_NAME)) {
			attributes.add(new Attribute("name", "Mario")
					.withColumnFamily(INDEX));
			items.add(new Row(new Key("id", "1"), attributes));

			attributes = new ArrayList<>();
			attributes.add(new Attribute("name", "Bowser")
					.withColumnFamily(INDEX));
			attributes.add(new Attribute("type", "turtle")
					.withColumnFamily(INDEX));
			items.add(new Row(new Key("id", "2"), attributes));

			attributes = new ArrayList<>();
			attributes.add(new Attribute("name", "Peach")
					.withColumnFamily(INDEX));
			attributes.add(new Attribute("type", "princess")
					.withColumnFamily(INDEX));
			attributes.add(new Attribute("age", "23").withColumnFamily(INDEX));
			items.add(new Row(new Key("id", "3"), attributes));

			attributes = new ArrayList<>();
			attributes.add(new Attribute("name", "Daisy")
					.withColumnFamily(INDEX));
			attributes.add(new Attribute("type", "princess")
					.withColumnFamily(INDEX));
			attributes.add(new Attribute("age", "25").withColumnFamily(INDEX));
			items.add(new Row(new Key("id", "4"), attributes));

			attributes = new ArrayList<>();
			attributes.add(new Attribute("name", "Yoshi")
					.withColumnFamily(INDEX));
			attributes.add(new Attribute("type", "dinosaur")
					.withColumnFamily(INDEX));
			attributes.add(new Attribute("age", "42").withColumnFamily(INDEX));
			items.add(new Row(new Key("id", "5"), attributes));
		} else if (tableName.equals(TABLE_NAME2)) {
			attributes.add(new Attribute("name", "Link")
					.withColumnFamily(INDEX));
			attributes.add(new Attribute("status", "hero of time")
					.withColumnFamily(INDEX));
			items.add(new Row(new Key("id", "100"), attributes));

			attributes = new ArrayList<>();
			attributes.add(new Attribute("name", "Ganondorf")
					.withColumnFamily(INDEX));
			attributes.add(new Attribute("alias", "Ganon")
					.withColumnFamily(INDEX));
			items.add(new Row(new Key("id", "200"), attributes));
		}
		queryHandler.insertRows(tableName, items);
	}

	public static void deleteTestTables() {
		queryHandler.deleteTable(TABLE_NAME);
		queryHandler.deleteTable(TABLE_NAME2);
	}

	public static void createTestTables() {
		queryHandler.createTable(TABLE_NAME, INDEX);
		queryHandler.createTable(TABLE_NAME2, INDEX);
	}

	public static List<String> getTestTableNames(List<String> tableNames) {
		List<String> testTableNames = new ArrayList<>();
		for (String tableName : tableNames) {
			if (tableName.indexOf("test_") > -1
					&& tableName.indexOf("^test_") == -1) {
				testTableNames.add(tableName);
			}
		}

		return testTableNames;
	}

	public static Map<String, ArrayList<Map<String, String>>> createKeys() {
		Map<String, ArrayList<Map<String, String>>> tableNamesWithKeys = new HashMap<>();
		ArrayList<Map<String, String>> keyList = new ArrayList<>();

		Map<String, String> keys = new HashMap<>();
		keys.put("id", "1");
		keyList.add(keys);

		keys = new HashMap<>();
		keys.put("id", "2");
		keyList.add(keys);
		tableNamesWithKeys.put(TestHandler.TABLE_NAME, keyList);

		keys = new HashMap<>();
		keys.put("id", "100");
		keyList.add(keys);

		keys = new HashMap<>();
		keys.put("id", "200");
		keyList.add(keys);
		tableNamesWithKeys.put(TestHandler.TABLE_NAME2, keyList);
		return tableNamesWithKeys;
	}

	public static void printRows(List<Row> rows) {
		for (Row row : rows) {
			System.out.println("---");
			for (Attribute att : row.getAttributes()) {
				System.out.println(att.toString());
			}
			System.out.println("---");
		}
	}
}
