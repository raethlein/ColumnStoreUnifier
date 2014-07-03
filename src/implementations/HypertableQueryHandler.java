package implementations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import model.Attribute;
import model.Filter;
import model.Row;

import org.apache.thrift.TException;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.ClientException;
import org.hypertable.thriftgen.HqlResult;

public class HypertableQueryHandler {

	public static void alterTableAddColumn(String tableName,
			String columnFamilyName) {
		try {
			String queryString = String.format("ALTER TABLE %s ADD (%s)",
					tableName, columnFamilyName);
			HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE,
					queryString);
		} catch (ClientException | TException e) {
			e.printStackTrace();
		}
	}

	public static void createNamespace(String namespaceName) {
		try {
			HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE,
					"CREATE NAMESPACE " + namespaceName);
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
			String queryString = String.format(
					"CREATE TABLE %s (%s, INDEX %2$s)", tableName,
					indexedColumnFamily);
			HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE,
					queryString);
		} catch (ClientException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}

	public static List<String> listTables() {
		try {
			HqlResult result = HypertableHandler.CLIENT.hql_query(
					HypertableHandler.NAMESPACE, "show tables");
			return result.getResults();
		} catch (ClientException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}

		return null;
	}

	public static void deleteTable(String tableName) {
		try {
			String queryString = String.format("DROP TABLE IF EXISTS %s",
					tableName);
			HypertableHandler.CLIENT.hql_query(HypertableHandler.NAMESPACE,
					queryString);
		} catch (ClientException | TException e) {
			e.printStackTrace();
		}
	}

	public static void insertItems(String tableName, List<Row> items) {
		for (Row item : items) {
			String key = item.getKey().getValue();
			for (Attribute attribute : item.getAttributes()) {
				String queryString = String.format(
						"INSERT INTO %s VALUES (\"%s\", \"%s:%s\", \"%s\")",
						tableName, key, attribute.getColumnFamily(),
						attribute.getName(), attribute.getValue());
				try {
					HypertableHandler.CLIENT.hql_query(
							HypertableHandler.NAMESPACE, queryString);
				} catch (ClientException | TException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static Row getRowByKey(String tableName, String key) {
		String queryString = String.format(
				"SELECT * FROM %s WHERE ROW = \"%s\"", tableName, key);
		try {
			HqlResult result = HypertableHandler.CLIENT.hql_query(
					HypertableHandler.NAMESPACE, queryString);
			List<Attribute> attributes = new ArrayList<>();

			for (Cell cell : result.getCells()) {
				attributes.add(new Attribute(cell.key.column_qualifier
						.toString(), new String(cell.getValue())));
			}

			return new Row(attributes);
		} catch (ClientException | TException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static List<Row> getRowsByKeys(String tableName, List<String> keys) {
		String queryString = String.format("SELECT * FROM %s WHERE ROW = ",
				tableName);
		int counter = 0;
		for (String key : keys) {
			queryString += key;
			if (counter < keys.size()) {
				queryString += " OR ROW = ";
			}
			counter++;
		}
		try {
			HqlResult result = HypertableHandler.CLIENT.hql_query(
					HypertableHandler.NAMESPACE, queryString);
			List<Row> transformedResultList = new ArrayList<>();
			List<Attribute> attributes = null;

			for (Cell cell : result.getCells()) {
				attributes = new ArrayList<>();
				attributes.add(new Attribute(cell.key.column_qualifier
						.toString(), new String(cell.getValue())));
				transformedResultList.add(new Row(attributes));
			}
			return transformedResultList;
		} catch (ClientException | TException e) {
			e.printStackTrace();
		}
		return null;
	}

	// TODO: implement conditional select programmatically
	public static List<Row> scanTable(String tableName,
			String conditionalOperator, Filter[] filters) {
		Map<String, Filter> filterMap = new HashMap<>();
		for (Filter filter : filters) {
			filterMap.put(filter.getAttribute().getName(), filter);
		}

		String queryString = String.format("SELECT * FROM %s", tableName);
		try {
			HqlResult result = HypertableHandler.CLIENT.hql_query(
					HypertableHandler.NAMESPACE, queryString);
			List<Row> transformedResultList = new ArrayList<>();
			List<Attribute> attributes = new ArrayList<>();
			String tempRow = "";

			boolean passesCheck;
			boolean rowHasColumn;
			Iterator<Cell> resultIterator = result.getCellsIterator();

			while (resultIterator.hasNext()) {
				Cell cell = resultIterator.next();
				
				if ((!tempRow.equals(cell.key.row) && !tempRow.equals("")) || !resultIterator.hasNext()) {
					passesCheck = true;
					rowHasColumn = false;
					for (Attribute attr : attributes) {
						Filter filter = filterMap.get(attr.getName());
						if (filter != null) {
							rowHasColumn = true;
							if (!filter(filter, attr.getValue())) {
								passesCheck = false;
								break;
							}
						}
					}
					if (!rowHasColumn) {
						passesCheck = false;
					}
					if (passesCheck) {
						transformedResultList.add(new Row(attributes));
					}
					attributes = new ArrayList<>();
				}

				String columnName = new String(
						cell.key.column_qualifier.toString());
				String columnValue = new String(cell.getValue());
				attributes.add(new Attribute(columnName, columnValue));

				tempRow = cell.key.row;
			}

			return transformedResultList;
		} catch (ClientException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static boolean filter(Filter filter, String columnValue) {
		System.out.println(filter.getComparisonOperator());
		if (filter.getComparisonOperator().equals("=")) {
			if (columnValue.compareTo(filter.getAttribute().getValue()) == 0) {
				return true;
			}
		} 
		else if(filter.getComparisonOperator().equals("!=")){
			if (columnValue.compareTo(filter.getAttribute().getValue()) != 0) {
				return true;
			}
		}
		else {
			try {
				double parsedFilterValue = Double.parseDouble(filter
						.getAttribute().getValue());
				double parsedColumnValue = Double.parseDouble(columnValue);

				if (filter.getComparisonOperator().equals("<")) {
					if (parsedColumnValue < parsedFilterValue) {
						return true;
					}
				} else if (filter.getComparisonOperator().equals(">")) {
					if (parsedColumnValue > parsedFilterValue) {
						return true;
					}
				} else if (filter.getComparisonOperator().equals("<=")) {
					if (parsedColumnValue <= parsedFilterValue) {
						return true;
					}
				} else if (filter.getComparisonOperator().equals(">=")) {
					if (parsedColumnValue >= parsedFilterValue) {
						return true;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return false;
	}
}
