import static org.junit.Assert.assertEquals;

import java.util.List;

import middleware.ComparisonOperatorMapper.ComparisonOperator;
import model.Attribute;
import model.Filter;
import model.Key;
import model.Row;

import org.junit.Test;

public class HbaseTest {
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

		Filter[] filter = {new Filter(new Attribute("age", "23"),  ComparisonOperator.NE), 
				new Filter(new Attribute("type", "princess"),  ComparisonOperator.NE), new Filter(new Attribute("name", "Peach"), ComparisonOperator.NE)};
		
		List<Row> rows = TestHandler.queryHandler.getRows(TestHandler.TABLE_NAME, "OR", filter);
		
		assertEquals(2, rows.size());
		assertEquals("Daisy", rows.get(0).getAttributesMap().get("name").getValue());
		assertEquals("42", rows.get(1).getAttributesMap().get("age").getValue());
		assertEquals("age", rows.get(1).getAttributesMap().get("age").getName());
		
		Filter[] filter2 = {new Filter(new Attribute("name", "Mario"),  ComparisonOperator.EQ), new Filter(new Attribute("name", "Bowser"), ComparisonOperator.EQ)};
		
		rows = TestHandler.queryHandler.getRows(TestHandler.TABLE_NAME, "OR", filter2);
		
		assertEquals("Mario", rows.get(0).getAttributesMap().get("name").getValue());
		assertEquals("Bowser", rows.get(1).getAttributesMap().get("name").getValue());
		assertEquals("turtle", rows.get(1).getAttributesMap().get("type").getValue());
	}
}