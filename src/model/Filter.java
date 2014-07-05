package model;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import middleware.ComparisonOperatorMapper;
import middleware.ComparisonOperatorMapper.ComparisonOperator;

public class Filter {
	private Attribute attribute;
	private Object comparisonOperator;
	private String standardizedOperator;

	public Filter(Attribute attribute, ComparisonOperator comparisonOperator) {
		super();
		this.attribute = attribute;
		
		this.comparisonOperator = ComparisonOperatorMapper.mapConditionalOperator(comparisonOperator);
		this.setStandardizedOperator(ComparisonOperatorMapper.mapStandardizedConditionalOperator(comparisonOperator));
	}

	public Attribute getAttribute() {
		return this.attribute;
	}

	public void setAttribute(Attribute attribute) {
		this.attribute = attribute;
	}

	public CompareOp getComparisonOperatorAsHbaseCompareOperator() {
		return (CompareOp) comparisonOperator;
	}
	
	public String getComparisonOperator(){
		return (String) comparisonOperator;
	}

	public String getStandardizedOperator() {
		return standardizedOperator;
	}

	public void setStandardizedOperator(String standardizedOperator) {
		this.standardizedOperator = standardizedOperator;
	}
}
