import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import middleware.QueryHandler;
import middleware.UnifyingMiddleware;
import model.Attribute;
import model.Row;
import model.Key;


public class TestHandler {

	public static String TABLE_NAME = "test_mushroom_kingdom";
	public static String TABLE_NAME2 = "test_hyrule";
	public static String INDEX = "id";
	public static QueryHandler queryHandler = UnifyingMiddleware.getQueryHandler();

	public static void insertTestItems(String tableName) {
		List<Row> items = new ArrayList<>();
		List<Attribute> attributes = new ArrayList<>();

		if (tableName.equals(TABLE_NAME)) {
			attributes.add(new Attribute("name", "Mario").withColumnFamil(INDEX));
			items.add(new Row(new Key("id", "1"), attributes));

			attributes = new ArrayList<>();
			attributes.add(new Attribute("name", "Bowser").withColumnFamil(INDEX));
			attributes.add(new Attribute("type", "turtle").withColumnFamil(INDEX));
			items.add(new Row(new Key("id", "2"), attributes));

			attributes = new ArrayList<>();
			attributes.add(new Attribute("name", "Peach").withColumnFamil(INDEX));
			attributes.add(new Attribute("type", "princess").withColumnFamil(INDEX));
			attributes.add(new Attribute("age", "23").withColumnFamil(INDEX));
			items.add(new Row(new Key("id", "3"), attributes));

			attributes = new ArrayList<>();
			attributes.add(new Attribute("name", "Daisy").withColumnFamil(INDEX));
			attributes.add(new Attribute("type", "princess").withColumnFamil(INDEX));
			attributes.add(new Attribute("age", "25").withColumnFamil(INDEX));
			items.add(new Row(new Key("id", "4"), attributes));

			attributes = new ArrayList<>();
			attributes.add(new Attribute("name", "Yoshi").withColumnFamil(INDEX));
			attributes.add(new Attribute("type", "dinosaur").withColumnFamil(INDEX));
			attributes.add(new Attribute("age", "42").withColumnFamil(INDEX));
			items.add(new Row(new Key("id", "5"), attributes));
		}
		else if (tableName.equals(TABLE_NAME2)) {
			attributes.add(new Attribute("name", "Link").withColumnFamil(INDEX));
			attributes.add(new Attribute("status", "hero of time").withColumnFamil(INDEX));
			items.add(new Row(new Key("id", "100"), attributes));

			attributes = new ArrayList<>();
			attributes.add(new Attribute("name", "Ganondorf").withColumnFamil(INDEX));
			attributes.add(new Attribute("alias", "Ganon").withColumnFamil(INDEX));
			items.add(new Row(new Key("id", "200"), attributes));
		}
		queryHandler.insertRows(tableName, items);
	}

	public static void deleteTestTables() {
		System.out.println("ofjeoivj");
		queryHandler.deleteTable(TABLE_NAME);
		queryHandler.deleteTable(TABLE_NAME2);
		System.out.println("delete");
	}

	public static void createTestTables() {
		queryHandler.createTable(TABLE_NAME, INDEX);
		queryHandler.createTable(TABLE_NAME2, INDEX);
	}

	public static List<String> getTestTableNames(List<String> tableNames) {
		List<String> testTableNames = new ArrayList<>();
		for (String tableName : tableNames) {
			if (tableName.indexOf("test_") > -1 && tableName.indexOf("^test_") == -1) {
				testTableNames.add(tableName);
			}
		}

		return testTableNames;
	}
	
	public static Map<String, ArrayList<Map<String, String>>> createKeys(){
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
}
