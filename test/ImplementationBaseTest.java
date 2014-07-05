import static org.junit.Assert.assertEquals;

import java.util.List;

import middleware.ComparisonOperatorMapper.ComparisonOperator;
import middleware.NoSQLMiddleware;
import middleware.NoSQLMiddleware.Implementations;
import model.Attribute;
import model.Filter;
import model.Key;
import model.Row;

import org.junit.Test;

public class ImplementationBaseTest {

	@Test
	public void createTableTest() {
		TestHandler.deleteTestTables();
		assertEquals(
				0,
				TestHandler.getTestTableNames(
						TestHandler.queryHandler.getTableNames()).size());
		TestHandler.createTestTables();
		assertEquals(
				2,
				TestHandler.getTestTableNames(
						TestHandler.queryHandler.getTableNames()).size());
	}

	@Test
	public void getItemByKeyTest() {
		TestHandler.deleteTestTables();
		TestHandler.createTestTables();
		TestHandler.insertTestItems(TestHandler.TABLE_NAME);

		Row item = TestHandler.queryHandler.getRowByKey(TestHandler.TABLE_NAME,
				new Key("id", "2"));
		assertEquals("Bowser", item.getAttributesMap().get("name").getValue());
		assertEquals(true, item.getAttributesMap().containsKey("type"));
	}

	@Test
	public void testScan1() {
		TestHandler.deleteTestTables();
		TestHandler.createTestTables();
		TestHandler.insertTestItems(TestHandler.TABLE_NAME);

		Filter[] filters = { new Filter(new Attribute("age", "24"),
				ComparisonOperator.GE) };

		List<Row> rows = TestHandler.queryHandler.getRows(
				TestHandler.TABLE_NAME, "OR", filters);
		TestHandler.printRows(rows);
		assertEquals(2, rows.size());
		assertEquals("Daisy", rows.get(0).getAttributesMap().get("name")
				.getValue());
		assertEquals("42", rows.get(1).getAttributesMap().get("age").getValue());
		assertEquals("Yoshi", rows.get(1).getAttributesMap().get("name")
				.getValue());
		assertEquals(false, rows.get(1).getAttributesMap().containsKey("alias"));

		TestHandler.deleteTestTables();
		TestHandler.createTestTables();
		TestHandler.insertTestItems(TestHandler.TABLE_NAME);

		Filter[] filters2 = {
				new Filter(new Attribute("age", "24"), ComparisonOperator.GE),
				new Filter(new Attribute("type", "princess"),
						ComparisonOperator.EQ) };

		rows = TestHandler.queryHandler.getRows(TestHandler.TABLE_NAME, "AND",
				filters2);

		assertEquals(1, rows.size());
		assertEquals("Daisy", rows.get(0).getAttributesMap().get("name")
				.getValue());
	}

	@Test
	public void testScan2() {
		TestHandler.deleteTestTables();
		TestHandler.createTestTables();
		TestHandler.insertTestItems(TestHandler.TABLE_NAME);

		Filter[] filter = {
				new Filter(new Attribute("age", "23"), ComparisonOperator.NE),
				new Filter(new Attribute("type", "princess"),
						ComparisonOperator.NE),
				new Filter(new Attribute("name", "Peach"),
						ComparisonOperator.NE) };

		List<Row> rows = TestHandler.queryHandler.getRows(
				TestHandler.TABLE_NAME, "OR", filter);
TestHandler.printRows(rows);
		assertEquals(4, rows.size());
		if (NoSQLMiddleware.getUsedDatabase() == Implementations.Cassandra) {
			assertEquals("Mario", rows.get(3).getAttributesMap().get("name")
					.getValue());
			assertEquals("25", rows.get(0).getAttributesMap().get("age")
					.getValue());
			assertEquals("age", rows.get(0).getAttributesMap().get("age")
					.getName());
		} else if(NoSQLMiddleware.getUsedDatabase() == Implementations.Hypertable){
			assertEquals("Mario", rows.get(0).getAttributesMap().get("name")
					.getValue());
			assertEquals("25", rows.get(2).getAttributesMap().get("age")
					.getValue());
			assertEquals("age", rows.get(2).getAttributesMap().get("age")
					.getName());
		}
		else {
			assertEquals("Mario", rows.get(0).getAttributesMap().get("name")
					.getValue());
			assertEquals("25", rows.get(1).getAttributesMap().get("age")
					.getValue());
			assertEquals("age", rows.get(1).getAttributesMap().get("age")
					.getName());
		}

		Filter[] filter2 = {
				new Filter(new Attribute("name", "Mario"),
						ComparisonOperator.EQ),
				new Filter(new Attribute("type", "turtle"),
						ComparisonOperator.EQ) };

		rows = TestHandler.queryHandler.getRows(TestHandler.TABLE_NAME, "OR",
				filter2);

		if (NoSQLMiddleware.getUsedDatabase() == Implementations.Cassandra) {
			assertEquals("Mario", rows.get(1).getAttributesMap().get("name")
					.getValue());
			assertEquals("Bowser", rows.get(0).getAttributesMap().get("name")
					.getValue());
			assertEquals("turtle", rows.get(0).getAttributesMap().get("type")
					.getValue());
		} else {
			assertEquals("Mario", rows.get(0).getAttributesMap().get("name")
					.getValue());
			assertEquals("Bowser", rows.get(1).getAttributesMap().get("name")
					.getValue());
			assertEquals("turtle", rows.get(1).getAttributesMap().get("type")
					.getValue());
		}

	}
}
