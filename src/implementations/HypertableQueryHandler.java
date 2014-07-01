package implementations;

import java.util.ArrayList;
import java.util.List;

import model.Attribute;
import model.Filter;
import model.Row;

import org.apache.thrift.TException;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.ClientException;
import org.hypertable.thriftgen.HqlResult;


public class HypertableQueryHandler {
	
	public static void alterTableAddColumn(String tableName, String columnFamilyName){
		try {
			String queryString = String.format("ALTER TABLE %s ADD (%s)", tableName, columnFamilyName);
			HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE, queryString);
		} catch (ClientException | TException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void createNamespace(String namespaceName){
		try {
			HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE, "CREATE NAMESPACE " + namespaceName);
		} catch (ClientException | TException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create a table with given name. The table contains only one column family
	 * in which every column is sorted into.
	 * 
	 * @param tableName
	 * @param indexedColumnFamily
	 *            Only column family of the table.
	 */
	public static void createTable(String tableName, String indexedColumnFamily) {
		try {
			String queryString = String.format("CREATE TABLE %s (%s, INDEX %2$s)", tableName, indexedColumnFamily);
			HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE, queryString);
		}
		catch (ClientException e) {
			e.printStackTrace();
		}
		catch (TException e) {
			e.printStackTrace();
		}
	}

	public static List<String> listTables() {
		try {
			HqlResult result = HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE, "show tables");
			return result.getResults();
		}
		catch (ClientException e) {
			e.printStackTrace();
		}
		catch (TException e) {
			e.printStackTrace();
		}

		return null;
	}

	public static void deleteTable(String tableName) {
		try {
			String queryString = String.format("DROP TABLE IF EXISTS %s", tableName);
			HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE, queryString);
		}
		catch (ClientException | TException e) {
			e.printStackTrace();
		}
	}

	public static void insertItems(String tableName, List<Row> items) {
		for (Row item : items) {
			String key = item.getKey().getValue();
			for (Attribute attribute : item.getAttributes()) {
				String queryString = String.format("INSERT INTO %s VALUES (\"%s\", \"%s:%s\", \"%s\")",
						tableName, key, attribute.getColumnFamily(), attribute.getName(), attribute.getValue());
				try {
					HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE, queryString);
				}
				catch (ClientException | TException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static Row getRowByKey(String tableName, String key) {
		String queryString = String.format("SELECT * FROM %s WHERE ROW = \"%s\"", tableName, key);
		try {
			HqlResult result = HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE, queryString);
			List<Attribute> attributes = new ArrayList<>();

			for (Cell cell : result.getCells()) {
				attributes.add(new Attribute(cell.key.column_qualifier.toString(), new String(cell.getValue())));
			}
			
			return new Row(attributes);
		}
		catch (ClientException | TException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static List<Row> getRowsByKeys(String tableName, List<String> keys) {
		String queryString = String.format("SELECT * FROM %s WHERE ROW = ", tableName);
		int counter = 0;
		for (String key : keys) {
			queryString += key;
			if (counter < keys.size()) {
				queryString += " OR ROW = ";
			}
			counter++;
		}
		try {
			HqlResult result = HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE, queryString);
			List<Row> transformedResultList = new ArrayList<>();
			List<Attribute> attributes = null;

			for (Cell cell : result.getCells()) {
				attributes = new ArrayList<>();
				attributes.add(new Attribute(cell.key.column_qualifier.toString(), new String(cell.getValue())));
				transformedResultList.add(new Row(attributes));
			}
			return transformedResultList;
		}
		catch (ClientException | TException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static List<Row> scanTable(String tableName, Filter filter) {
		String queryString = String.format("SELECT %s FROM %s WHERE %1$s = \"%s\"", 
				filter.getAttribute().getColumnFamily(), tableName, filter.getAttribute().getValue());

		try {
			HqlResult result = HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE, queryString);
			List<Row> transformedResultList = new ArrayList<>();
			List<Attribute> attributes = null;

			for (Cell cell : result.getCells()) {
				attributes = new ArrayList<>();
				attributes.add(new Attribute(cell.key.column_qualifier.toString(), new String(cell.getValue())));
				transformedResultList.add(new Row(attributes));
			}
			return transformedResultList;
		}
		catch (ClientException e) {
			e.printStackTrace();
		}
		catch (TException e) {
			e.printStackTrace();
		}
		return null;
	}
}
