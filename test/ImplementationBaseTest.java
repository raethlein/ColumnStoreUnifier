import static org.junit.Assert.assertEquals;
import model.Key;
import model.Row;

import org.junit.Test;


public class ImplementationBaseTest {

	@Test
	public void createTableTest() {
		TestHandler.deleteTestTables();
		assertEquals(0, TestHandler.getTestTableNames(TestHandler.queryHandler.getTableNames()).size());
		TestHandler.createTestTables();
		assertEquals(2, TestHandler.getTestTableNames(TestHandler.queryHandler.getTableNames()).size());
	}
	
	@Test
	public void getItemByKeyTest() {
		TestHandler.deleteTestTables();
		TestHandler.createTestTables();
		TestHandler.insertTestItems(TestHandler.TABLE_NAME);

		Row item = TestHandler.queryHandler.getRowByKey(TestHandler.TABLE_NAME, new Key("id", "2"));
		assertEquals("Bowser", item.getAttributesMap().get("name").getValue());
		assertEquals(true, item.getAttributesMap().containsKey("type"));
	}
}
