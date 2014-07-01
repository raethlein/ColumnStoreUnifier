package model;

public class Filter {
	private String attributeName;
	private String comparisonOperator;
	private String attributeValue;
	
	public Filter(String attributeName, String comparisonOperator, String attributeValue) {
		super();
		this.attributeName = attributeName;
		this.comparisonOperator = comparisonOperator;
		this.attributeValue = attributeValue;
	}

	public String getAttributeName() {
		return attributeName;
	}
	
	public void setAttributeName(String attributeName) {
		this.attributeName = attributeName;
	}
	
	public String getComparisonOperator() {
		return comparisonOperator;
	}
	
	public void setComparisonOperator(String comparisonOperator) {
		this.comparisonOperator = comparisonOperator;
	}
	
	public String getAttributeValue() {
		return attributeValue;
	}
	
	public void setAttributeValue(String attributeValue) {
		this.attributeValue = attributeValue;
	}

}
