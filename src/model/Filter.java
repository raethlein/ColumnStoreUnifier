package model;

public class Filter {
	private Attribute attribute;
	private String comparisonOperator;

	public Filter(Attribute attribute, String comparisonOperator) {
		super();
		this.attribute = attribute;
		this.comparisonOperator = comparisonOperator;
	}

	public Attribute getAttribute() {
		return this.attribute;
	}

	public void setAttribute(Attribute attribute) {
		this.attribute = attribute;
	}

	public String getComparisonOperator() {
		return comparisonOperator;
	}

	public void setComparisonOperator(String comparisonOperator) {
		this.comparisonOperator = comparisonOperator;
	}
}
