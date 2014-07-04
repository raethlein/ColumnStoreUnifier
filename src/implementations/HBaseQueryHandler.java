package implementations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import model.Attribute;
import model.Filter;
import model.Key;
import model.Row;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseQueryHandler {

	public static void createNamespace(String namespaceName) {
		try {
			HBaseAdmin hbaseAdmin = new HBaseAdmin(HBaseHandler.config);
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		NamespaceDescriptor namespace = NamespaceDescriptor.create(namespaceName).build();
		
	}

	/**
	 * The primary key will not be the primary key of the table. HBase itself defines a primary key column,
	 * it is not possible to create another key column or a secondary index.
	 * The primary key will be the first column family of the new table.
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
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public static void alterTableAddColumnFamily(String tableName,
			String columnName) {
		HBaseAdmin hbaseAdmin;
		try {
			hbaseAdmin = new HBaseAdmin(HBaseHandler.config);
//			HTableDescriptor desc = new HTableDescriptor(tableName);
			HColumnDescriptor columnDesc = new HColumnDescriptor(columnName);
//			desc.addFamily(columnDesc);
			hbaseAdmin.addColumn(tableName, columnDesc);
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

	public static void deleteTable(String tableName) {
		HBaseAdmin hbaseAdmin;
		try {
			hbaseAdmin = new HBaseAdmin(HBaseHandler.config);
			hbaseAdmin.deleteTable(tableName);
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public static void insertItems(String tableName, List<Row> items) {
		HBaseAdmin hbaseAdmin;
		try {
			hbaseAdmin = new HBaseAdmin(HBaseHandler.config);
			HTable table = new HTable(HBaseHandler.config, tableName);
			for (Row row: items) {
				Put p = new Put(Bytes.toBytes(row.getKey().getValue()));
				Collection<Attribute> attributes = row.getAttributes();
				for (Attribute attribute : attributes) {
					p.add(Bytes.toBytes(attribute.getColumnFamily()), Bytes.toBytes(attribute.getName()), Bytes.toBytes(attribute.getValue()));
					
				}
				table.put(p);
			}			
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
				attributes.add(new Attribute(new String(kv.getRow()), new String(kv.getValue())));
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new Row(attributes);
	}

	public static List<Row> scanTable(String tableName, Filter[] filters,
			String conditionalOperator) {
		// TODO Auto-generated method stub
		return null;
	}

}
