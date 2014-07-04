package implementations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

public class HBaseHandler {
	static Configuration config;
	public static void connect() {
		config = HBaseConfiguration.create();
	}

}
