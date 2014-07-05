package implementations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import model.Attribute;
import model.Filter;
import model.Key;
import model.Row;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseQueryHandler {

	public static void createNamespace(String namespaceName) {
		try {
			HBaseAdmin hbaseAdmin = new HBaseAdmin(HBaseHandler.config);
			hbaseAdmin.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
		// NamespaceDescriptor namespace =
		// NamespaceDescriptor.create(namespaceName).build();
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * The primary key will not be the primary key of the table. HBase itself
	 * defines a primary key column, it is not possible to create another key
	 * column or a secondary index. The primary key will be the first column
	 * family of the new table.
	 * 
	 * @param tableName
	 * @param primaryKey
	 */
	public static void createTable(String tableName, String primaryKey) {
		try {
			HBaseAdmin hbaseAdmin = new HBaseAdmin(HBaseHandler.config);
			HTableDescriptor desc = new HTableDescriptor(tableName);
			HColumnDescriptor columnDesc = new HColumnDescriptor(primaryKey);
			desc.addFamily(columnDesc);
			hbaseAdmin.createTable(desc);
			hbaseAdmin.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void alterTableAddColumnFamily(String tableName,
			String columnName) {
		HBaseAdmin hbaseAdmin;
		try {
			hbaseAdmin = new HBaseAdmin(HBaseHandler.config);
			// HTableDescriptor desc = new HTableDescriptor(tableName);
			HColumnDescriptor columnDesc = new HColumnDescriptor(columnName);
			// desc.addFamily(columnDesc);
			hbaseAdmin.addColumn(tableName, columnDesc);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static List<String> getTableNames() {
		HBaseAdmin hbaseAdmin;
		List<String> result = new ArrayList<String>();
		try {
			hbaseAdmin = new HBaseAdmin(HBaseHandler.config);
			HTableDescriptor[] tables = hbaseAdmin.listTables();
			for (HTableDescriptor table : tables) {
				result.add(table.getNameAsString());
			}
			hbaseAdmin.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	public static void deleteTable(String tableName) {
		HBaseAdmin hbaseAdmin;
		try {
			hbaseAdmin = new HBaseAdmin(HBaseHandler.config);
			hbaseAdmin.disableTable(tableName);
			hbaseAdmin.deleteTable(tableName);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void insertItems(String tableName, List<Row> items) {
		HBaseAdmin hbaseAdmin;
		try {
			hbaseAdmin = new HBaseAdmin(HBaseHandler.config);
			HTable table = new HTable(HBaseHandler.config, tableName);
			for (Row row : items) {
				Put put = new Put(Bytes.toBytes(row.getKey().getValue()));
				Collection<Attribute> attributes = row.getAttributes();
				for (Attribute attribute : attributes) {
					put.add(Bytes.toBytes(attribute.getColumnFamily()),
							Bytes.toBytes(attribute.getName()),
							Bytes.toBytes(attribute.getValue()));

				}
				table.put(put);
			}
			table.flushCommits();
			table.close();
			hbaseAdmin.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static Row getRowByKey(String tableName, Key[] combinedKey) {
		HBaseAdmin hbaseAdmin;
		HTable table;
		Result getResult;
		List<Attribute> attributes = new ArrayList<Attribute>();
		try {
			hbaseAdmin = new HBaseAdmin(HBaseHandler.config);
			table = new HTable(HBaseHandler.config, tableName);
			Get get = new Get(Bytes.toBytes(combinedKey[0].getValue()));
			getResult = table.get(get);
			List<KeyValue> kvs = getResult.list();
			for (KeyValue kv : kvs) {
				attributes.add(new Attribute(new String(kv.getQualifier()),
						new String(kv.getValue())));
			}
			table.close();
			hbaseAdmin.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new Row(attributes);
	}

	public static List<Row> scanTable(String tableName, Filter[] filters,
			String conditionalOperator) {
		HTable table;
		try {
			table = new HTable(HBaseHandler.config, tableName);

			ResultScanner scan = table.getScanner(new Scan());
			ArrayList<Filter> filterList = new ArrayList<>();
			for (Filter filter : filters) {
				filterList.add(filter);
			}
			Result result;
			boolean passesCheck;
			int numberOfPassedFilters;
			int numberOfFittingColumns;
			List<Row> transformedResultList = new ArrayList<>();
			List<Attribute> attributes = new ArrayList<>();
			List<KeyValue> keyValueList;
			passesCheck = true;
			while ((result = scan.next()) != null) {
				System.out.println("baar!");
				keyValueList = new ArrayList<>();
				for (KeyValue kv : result.raw()) {
					keyValueList.add(kv);
				}
				numberOfPassedFilters = 0;
				numberOfFittingColumns = 0;
				Iterator<KeyValue> resultIterator = keyValueList.iterator();
				attributes = new ArrayList<>();

				while (resultIterator.hasNext()) {
					KeyValue kv = resultIterator.next();
					String key = new String(kv.getQualifier());
					String value = new String(kv.getValue());
					System.out.println(key + " " + value);
					attributes.add(new Attribute(key, value));
				}
				
				for (Attribute attr : attributes) {
					for (Filter filter : filterList) {
						if (attr.getName().equals(
								filter.getAttribute().getName())) {
							numberOfFittingColumns++;
							if (!filter(filter, attr.getValue())) {
								passesCheck = false;
								// break;
							} else {
								numberOfPassedFilters++;
							}
						}
					}
				}
				if (conditionalOperator.equals("OR")) {
					if (numberOfPassedFilters == 0) {
						passesCheck = false;
					} else {
						passesCheck = true;
					}
				} else if (conditionalOperator.equals("AND")) {
					if (numberOfPassedFilters != filterList.size()) {
						passesCheck = false;
					}
				}
				if (numberOfFittingColumns < filterList.size()) {
					passesCheck = false;
				}
				if (passesCheck) {
					for (Attribute attr : attributes) {
						System.out.println(attr.toString());
					}
					transformedResultList.add(new Row(attributes));
				}
			}

			scan.close();
			table.close();
			return transformedResultList;

		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static boolean filter(Filter filter, String columnValue) {
		if (filter.getStandardizedOperator().equals("=")) {
			if (columnValue.compareTo(filter.getAttribute().getValue()) == 0) {
				return true;
			}
		} else if (filter.getStandardizedOperator().equals("!=")) {
			if (columnValue.compareTo(filter.getAttribute().getValue()) != 0) {
				return true;
			}
		} else {
			try {
				double parsedFilterValue = Double.parseDouble(filter
						.getAttribute().getValue());
				double parsedColumnValue = Double.parseDouble(columnValue);

				if (filter.getStandardizedOperator().equals("<")) {
					if (parsedColumnValue < parsedFilterValue) {
						return true;
					}
				} else if (filter.getStandardizedOperator().equals(">")) {
					if (parsedColumnValue > parsedFilterValue) {
						return true;
					}
				} else if (filter.getStandardizedOperator().equals("<=")) {
					if (parsedColumnValue <= parsedFilterValue) {
						return true;
					}
				} else if (filter.getStandardizedOperator().equals(">=")) {
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
