import static org.junit.Assert.assertEquals;

import java.util.List;

import model.Attribute;
import model.Filter;
import model.Key;
import model.Row;

import org.junit.Test;



public class HypertableTest {
	@Test
	public void createTableTest() {
		TestHandler.deleteTestTables();
		assertEquals(0, TestHandler.getTestTableNames(TestHandler.queryHandler.getTableNames()).size());
		TestHandler.createTestTables();
		assertEquals(2, TestHandler.getTestTableNames(TestHandler.queryHandler.getTableNames()).size());
	}

	@Test
	public void getRowByKeyTest() {
		TestHandler.deleteTestTables(); 
		TestHandler.createTestTables();
		TestHandler.insertTestItems(TestHandler.TABLE_NAME);

		Row row = TestHandler.queryHandler.getRowByKey(TestHandler.TABLE_NAME, new Key("id", "2"));
		
		assertEquals("Bowser", row.getAttributesMap().get("name").getValue());
		assertEquals(true, row.getAttributesMap().containsKey("type"));
	}
	
	@Test
	public void selectTest(){
		TestHandler.deleteTestTables();
		TestHandler.createTestTables();
		TestHandler.insertTestItems(TestHandler.TABLE_NAME);

		Filter filter = new Filter(new Attribute("type", "princess").withColumnFamily("id"), "=");
		
		List<Row> rows = TestHandler.queryHandler.getRows(TestHandler.TABLE_NAME, "AND", filter);

		assertEquals("Daisy", rows.get(4).getAttributesMap().get("name").getValue());
	}
}
