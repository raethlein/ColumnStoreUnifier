import static org.junit.Assert.assertEquals;

import java.util.List;

import model.Attribute;
import model.Filter;
import model.Key;
import model.Row;

import org.junit.Test;


public class CassandraTest {

	@Test
	public void createTableTest() {
		TestHandler.deleteTestTables();
		assertEquals(0, TestHandler.getTestTableNames(TestHandler.queryHandler.getTableNames()).size());
		TestHandler.createTestTables();
		assertEquals(2, TestHandler.getTestTableNames(TestHandler.queryHandler.getTableNames()).size());
	}
	
	@Test
	public void getItemTest() {
		TestHandler.deleteTestTables();
		TestHandler.createTestTables();
		TestHandler.insertTestItems(TestHandler.TABLE_NAME);

		Key key = new Key("id", "2");

		Row item = TestHandler.queryHandler.getRowByKey(TestHandler.TABLE_NAME, key);
		assertEquals("Bowser", item.getAttributesMap().get("name").getValue());
		assertEquals(true, item.getAttributesMap().containsKey("type"));
	}
	
	@Test
	public void scanTest(){
		TestHandler.deleteTestTables();
		TestHandler.createTestTables();
		TestHandler.insertTestItems(TestHandler.TABLE_NAME);
		
		Filter filter1 = new Filter(new Attribute("age", "24"), "GE");
		Filter filter2 = new Filter(new Attribute("type", "princess"), "EQ");
		
		List<Row> items = TestHandler.queryHandler.getRows(TestHandler.TABLE_NAME, "OR", filter1, filter2);
		
		assertEquals(3, items.size());
		assertEquals("Daisy", items.get(0).getAttributesMap().get("name").getValue());
		assertEquals("Peach", items.get(1).getAttributesMap().get("name").getValue());
		assertEquals(true, items.get(1).getAttributesMap().containsKey("type"));
		assertEquals("42", items.get(2).getAttributesMap().get("age").getValue());
		assertEquals("Yoshi", items.get(2).getAttributesMap().get("name").getValue());
		assertEquals(false, items.get(1).getAttributesMap().containsKey("alias"));
		
		TestHandler.deleteTestTables();
		TestHandler.createTestTables();
		TestHandler.insertTestItems(TestHandler.TABLE_NAME);
		
		filter1 = new Filter(new Attribute("age", "24"), "GE");
		filter2 = new Filter(new Attribute("type", "princess"), "EQ");
		
		items = TestHandler.queryHandler.getRows(TestHandler.TABLE_NAME, "AND", filter1, filter2);
		
		assertEquals(1, items.size());
		assertEquals("Daisy", items.get(0).getAttributesMap().get("name").getValue());
	}
}
