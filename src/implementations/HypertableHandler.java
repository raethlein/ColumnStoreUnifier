package implementations;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import interfaces.MiddlewareInterface;
import model.Filter;
import model.Key;
import model.Row;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.ClientException;

public class HypertableHandler implements MiddlewareInterface {
	public static ThriftClient CLIENT;
	public static long NAMESPACE;
	
	public void connectToDatabase(String host, String port){
		try {
			CLIENT = ThriftClient.create(host, Integer.parseInt(port));
			NAMESPACE = CLIENT.namespace_open("test");
		}
		catch (TTransportException e) {
			e.printStackTrace();
		}
		catch (TException e) {
			e.printStackTrace();
		}
		catch (ClientException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void alterTableAddColumn(String tableName, String columnName) {
		HypertableQueryHandler.alterTableAddColumn(tableName, columnName);
	}

	@Override
	public void createNamespace(String namespaceName) {
		HypertableQueryHandler.createNamespace(namespaceName);
	}

	@Override
	public void createTable(String tableName, String primaryKey) {
		HypertableQueryHandler.createTable(tableName, primaryKey);
	}

	@Override
	public void deleteTable(String tableName) {
		HypertableQueryHandler.deleteTable(tableName);
	}

	@Override
	public void insertRows(String tableName, List<Row> rows) {
		HypertableQueryHandler.insertItems(tableName, rows);
	}

	@Override
	public Row getRowByKey(String tableName, Key... combinedKey) {
		return HypertableQueryHandler.getRowByKey(tableName, combinedKey[0].getValue());
	}

	@Override
	public List<Row> getRowsByKeys(
			Map<String, ArrayList<Map<String, String>>> tableNamesWithKeys) {
		return null;
	}

	@Override
	public List<Row> getRows(String tableName, String conditionalOperator,
			Filter... filters) {
		return HypertableQueryHandler.scanTable(tableName, conditionalOperator, filters);
	}

	@Override
	public List<String> getTableNames() {
		return HypertableQueryHandler.listTables();
	}

}
