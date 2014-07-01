import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import model.Filter;
import model.Key;
import model.Row;

import org.junit.Test;


public class DynamoDbTest {

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

		Row item = TestHandler.queryHandler.getRowByKey(TestHandler.TABLE_NAME, new Key("id", "2"));
		assertEquals("Bowser", item.getAttributesMap().get("name").getValue());
		assertEquals(true, item.getAttributesMap().containsKey("type"));
	}

	@Test
	public void getBatchItemTest() {
		TestHandler.deleteTestTables();
		TestHandler.createTestTables();
		TestHandler.insertTestItems(TestHandler.TABLE_NAME);
		TestHandler.insertTestItems(TestHandler.TABLE_NAME2);
		List<Row> items = TestHandler.queryHandler.getRowsByKeys(TestHandler.createKeys());
		
		assertEquals(4, items.size());		
		boolean mario = false;
		boolean link = false;
		boolean bowser = false;
		boolean ganondorf = false;
		
		for(Row item : items){
			if(item.getAttributesMap().get("name").getValue().equals("Mario")){
				assertEquals(false, item.getAttributesMap().containsKey("type"));
				mario = true;
			}
			else if(item.getAttributesMap().get("name").getValue().equals("Link")){
				assertEquals("hero of time", item.getAttributesMap().get("status").getValue());
				link = true;
			}
			else if(item.getAttributesMap().get("name").getValue().equals("Bowser")){
				assertEquals(true, item.getAttributesMap().containsKey("type"));
				bowser = true;
			}
			else if(item.getAttributesMap().get("name").getValue().equals("Ganondorf")){
				assertEquals(true, item.getAttributesMap().containsKey("alias"));
				assertEquals("200", item.getAttributesMap().get("id").getValue());		
				ganondorf = true;
			}
		}
		
		assertEquals(true, mario && link && bowser && ganondorf);
	}
	
	@Test
	public void scanTest(){
		TestHandler.deleteTestTables();
		TestHandler.createTestTables();
		TestHandler.insertTestItems(TestHandler.TABLE_NAME);
		
		ArrayList<Filter> filters = new ArrayList<>();
		Filter filter = new Filter("age", "GE", "24");
		filters.add(filter);
		filter = new Filter("type", "EQ", "princess");
		filters.add(filter);
		
		List<Row> items = TestHandler.queryHandler.getRows(TestHandler.TABLE_NAME, "OR", filters);
		
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
		
		filters = new ArrayList<>();
		filter = new Filter("age", "GE", "24");
		filters.add(filter);
		filter = new Filter("type", "EQ", "princess");
		filters.add(filter);
		
		items = TestHandler.queryHandler.getRows(TestHandler.TABLE_NAME, "AND", filters);
		
		assertEquals(1, items.size());
		assertEquals("Daisy", items.get(0).getAttributesMap().get("name").getValue());
	}
}
