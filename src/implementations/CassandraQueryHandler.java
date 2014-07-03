package implementations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import model.Attribute;
import model.Filter;
import model.Key;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;

public class CassandraQueryHandler {
	
	
	/**
	 * Creates the keyspace for a cassandra cluster. A keyspace can be seen as a database in a RDBMS.
	 * @param name The name of the keyspace
	 * @param replication The replication strategy as a JSON string. If null, a default JSON string is used.
	 */
	public static void createKeyspace(String keyspaceName, String replication) {
		String query = "CREATE KEYSPACE IF NOT EXISTS ";
		query += keyspaceName;
		query += " WITH replication ";
		if (replication == null) {
			query += "{'class': 'SimpleStrategy', 'replication_factor':'3'}";
		} else {
			query += replication;
		}
		CassandraHandler.session.execute(query);
	}
	
	/**
	 * Creates a keyspace for a cassandra cluster with a default replication.
	 * 
	 * @param name The name of the keyspace
	 */
	public static void createKeyspace(String keyspaceName) {
		createKeyspace(keyspaceName, null);
	}
	
	
	/**
	 * Creates a table in a keyspace. Everything is saved as a blob, 
	 * so the client has to handle the converting from and to bytes.
	 * 
	 * @param keyspace The name of the keyspace
	 * @param tableName The name of the table to be created
	 * @param columns The columns of the table to be created. 
	 * @param primaryKeys The primary key(s) of the table. Those are the name of the columns which make up the primary key
	 */
	public static void createTable(String tableName, List<String> columns, List<String> primaryKeys) {
		String query = "CREATE TABLE IF NOT EXISTS "+tableName+" (";
		for(String column : columns) {
			query += column +" text,";
		}
		query += "PRIMARY KEY (";
		for (String column : primaryKeys) {
			query += column +",";
		}
		query.substring(0, query.length()-1);
		query += ");";
		CassandraHandler.session.execute(query);
	}
	
	public static void createTable(String tableName, String primaryKey) {
		String query = "CREATE TABLE IF NOT EXISTS "+tableName+" (";
		query += primaryKey+" int PRIMARY KEY);";
		CassandraHandler.session.execute(query);
	}
	
	/**
	 * Deletes a table in the current keyspace
	 * 
	 * @param table The table to be deleted
	 */
	public static void deleteTable(String table) {
		String query = "DROP TABLE IF EXISTS "+table;
		CassandraHandler.session.execute(query);
	}
	
	public static void alterTable(String tableName, String instruction) {
		String query = "ALTER TABLE "+tableName+" "+instruction;
		CassandraHandler.session.execute(query);
	}
	public static void alterTableAddColumn(String tableName, String column) {
		alterTable(tableName, " ADD "+column+" text");
	}
	
	/**
	 * Fetches the table names of the current keyspace
	 * @return A list of all table names.
	 */
	public static List<String> getTableNames() {
		
		Collection<TableMetadata> tables = CassandraHandler.cluster.getMetadata().getKeyspace(CassandraHandler.keyspace).getTables();
		ArrayList<String> tableNames = new ArrayList<String>(); 
		for (TableMetadata table : tables) {
			tableNames.add(table.getName());
		}
		return tableNames;
	}
	
	/**
	 * Inserts data into the table of the current keyspace.
	 * 
	 * @param table The table where the data is inserted
	 * @param columns The columns the row has. The primary key columns have to be provided
	 * @param values The values of the columns
	 */
	public static void insertItems(String table, List<model.Row> items) {
		for(model.Row item : items) {
			insertItem(table, item);
		}
	}
	
	public static void insertItem(String table, model.Row item) {
		String query = "INSERT INTO "+table+" (";
		Collection<Attribute> attributes = item.getAttributes();
		
		List<ColumnMetadata> columns = CassandraHandler.cluster.getMetadata().getKeyspace(CassandraHandler.keyspace).getTable(table).getColumns();
		List<String> columnNames = new ArrayList<String>();
		for (ColumnMetadata column : columns) {
			columnNames.add(column.getName());
		}
		query += item.getKey().getName()+",";
		for (Attribute attribute : attributes) {
			if(!columnNames.contains(attribute.getName())) {
				alterTableAddColumn(table, attribute.getName());
			}
			query += " "+attribute.getName()+",";
		}
		query = query.substring(0, query.length()-1);
		query += ") VALUES ("+item.getKey().getValue()+", ";
		for (Attribute attribute : attributes) {
			query += " '"+attribute.getValue()+"',";
		}
		query = query.substring(0, query.length()-1);
		query += " );";
		System.out.println(query);
		CassandraHandler.session.execute(query);
	}
	
	public static model.Row getRowByKey(String tableName, Key... keys) {
		String query = "SELECT * ";
		query += " FROM "+tableName;
		query += " WHERE ";
		for(Key key : keys) {
			query += key.getName()+" = "+key.getValue()+" AND ";
		}
		query = query.substring(0, query.length()-4);
		
		ResultSet results = CassandraHandler.session.execute(query);
		Row row = results.one();
		
		List<ColumnDefinitions.Definition> columns = new LinkedList<ColumnDefinitions.Definition>(row.getColumnDefinitions().asList());
		List<Attribute> attributes = new ArrayList<Attribute>();
		
		ColumnDefinitions.Definition definition = columns.get(0);
		Key key = new Key(definition.getName(), String.valueOf(row.getInt(definition.getName())));
		columns.remove(0);
		
		for(ColumnDefinitions.Definition def: columns) {
			Attribute attribute = new Attribute(def.getName(), row.getString(def.getName()));
			attributes.add(attribute);
		}
		model.Row result = new model.Row(key, attributes);
		return result;	
	}
	
	public static List<model.Row> scanTable(String tableName, String conditionalOperator, Filter... filters) {
		String query = "SELECT * FROM " + tableName + " WHERE ";
		for (Filter filter : filters) {
			query += filter.getAttribute().getName()+" "+filter.getComparisonOperator()+" "+filter.getAttribute().getValue();
			query += " "+conditionalOperator+" ";
		}
		query = query.substring(0, query.length()-conditionalOperator.length()-1);
		query += ";";
		System.out.println(query);
		ResultSet resultsFromDB = CassandraHandler.session.execute(query);
		List<Row> rows = resultsFromDB.all();
		List<ColumnDefinitions.Definition> columns = rows.get(0).getColumnDefinitions().asList();
		
		List<model.Row> result = new ArrayList<model.Row>();
		for(Row row : rows) {
			List<Attribute> attributes = new ArrayList<Attribute>();
			for (ColumnDefinitions.Definition def : columns) {
				Attribute attribute = new Attribute(def.getName(), row.getString(def.getName()));
			}
			result.add(new model.Row(attributes));
		}
		return result;
	}
}
