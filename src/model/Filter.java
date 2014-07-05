package model;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import middleware.ComparisonOperatorMapper;
import middleware.ComparisonOperatorMapper.ComparisonOperator;

public class Filter {
	private Attribute attribute;
	private Object comparisonOperator;

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

	public CompareOp getComparisonOperatorAsHbaseCompareOperator() {
		return (CompareOp) comparisonOperator;
	}
	
	public String getComparisonOperator(){
		return (String) comparisonOperator;
	}
}
