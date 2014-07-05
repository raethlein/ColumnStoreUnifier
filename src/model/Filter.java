package model;

import middleware.ComparisonOperatorMapper;
import middleware.ComparisonOperatorMapper.ComparisonOperator;

public class Filter {
	private Attribute attribute;
	private String comparisonOperator;

	public Filter(Attribute attribute, ComparisonOperator comparisonOperator) {
		super();
		this.attribute = attribute;
		this.comparisonOperator = ComparisonOperatorMapper.mapConditionalOperator(comparisonOperator);
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
