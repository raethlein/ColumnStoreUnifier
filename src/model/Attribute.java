package model;


public class Attribute {
	private String name;
	private String value;
	
	/**
	 * Only needed for Hypertable
	 */
	private String columnFamily;
	
	public Attribute(String name, String value){
		this.setName(name);
		this.setValue(value);
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}
	
	public Attribute withColumnFamil(String columnFamily){
		this.columnFamily = columnFamily;
		return this;
	}
}
