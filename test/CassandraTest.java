import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import model.Filter;
import model.Row;

import org.junit.Test;


public class CassandraTest {

//	@Test
//	public void createTableTest() {
//		TestHandler.deleteTestTables();
//		assertEquals(0, TestHandler.getTestTableNames(TestHandler.queryHandler.getTableNames()).size());
//		TestHandler.createTestTables();
//		assertEquals(2, TestHandler.getTestTableNames(TestHandler.queryHandler.getTableNames()).size());
//	}
	
//	@Test
//	public void getItemTest() {
//		TestHandler.deleteTestTables();
//		TestHandler.createTestTables();
//		TestHandler.insertTestItems(TestHandler.TABLE_NAME);
//
//		Map<String, String> key = new HashMap<>();
//		key.put("id", "2");
//
//		Row item = TestHandler.queryHandler.getRowByKey(TestHandler.TABLE_NAME, key);
//		assertEquals("Bowser", item.getAttributesMap().get("name").getValue());
//		assertEquals(true, item.getAttributesMap().containsKey("type"));
//	}
	
//	@Test
//	public void scanTest(){
//		TestHandler.deleteTestTables();
//		TestHandler.createTestTables();
//		TestHandler.insertTestItems(TestHandler.TABLE_NAME);
//		
//		ArrayList<Filter> filters = new ArrayList<>();
//		Filter filter = new Filter("age", "GE", "24");
//		filters.add(filter);
//		filter = new Filter("type", "EQ", "princess");
//		filters.add(filter);
//		
//		List<Row> items = TestHandler.queryHandler.getRows(TestHandler.TABLE_NAME, "OR", filters);
//		
//		assertEquals(3, items.size());
//		assertEquals("Daisy", items.get(0).getAttributesMap().get("name").getValue());
//		assertEquals("Peach", items.get(1).getAttributesMap().get("name").getValue());
//		assertEquals(true, items.get(1).getAttributesMap().containsKey("type"));
//		assertEquals("42", items.get(2).getAttributesMap().get("age").getValue());
//		assertEquals("Yoshi", items.get(2).getAttributesMap().get("name").getValue());
//		assertEquals(false, items.get(1).getAttributesMap().containsKey("alias"));
//		
//		TestHandler.deleteTestTables();
//		TestHandler.createTestTables();
//		TestHandler.insertTestItems(TestHandler.TABLE_NAME);
//		
//		filters = new ArrayList<>();
//		filter = new Filter("age", "GE", "24");
//		filters.add(filter);
//		filter = new Filter("type", "EQ", "princess");
//		filters.add(filter);
//		
//		items = TestHandler.queryHandler.getRows(TestHandler.TABLE_NAME, "AND", filters);
//		
//		assertEquals(1, items.size());
//		assertEquals("Daisy", items.get(0).getAttributesMap().get("name").getValue());
//	}
}
