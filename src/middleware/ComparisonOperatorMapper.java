package middleware;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class ComparisonOperatorMapper {
	public enum ComparisonOperator {
		EQ,
		NE,
		LT,
		GT,
		LE,
		GE
	}
	
	private static Map<ComparisonOperator, String> wordOperators = new HashMap<>();
	private static Map<ComparisonOperator, String> charOperator = new HashMap<>();
	private static Map<ComparisonOperator, CompareFilter.CompareOp> constantOperator = new HashMap<>();
	
	public static void initConditionalOperatorMapper(){
		wordOperators.put(ComparisonOperator.EQ, "EQ");
		wordOperators.put(ComparisonOperator.NE, "NE");
		wordOperators.put(ComparisonOperator.LT, "LT");
		wordOperators.put(ComparisonOperator.GT, "GT");
		wordOperators.put(ComparisonOperator.LE, "LE");
		wordOperators.put(ComparisonOperator.GE, "GE");
		
		charOperator.put(ComparisonOperator.EQ, "=");
		charOperator.put(ComparisonOperator.NE, "!=");
		charOperator.put(ComparisonOperator.LT, "<");
		charOperator.put(ComparisonOperator.GT, ">");
		charOperator.put(ComparisonOperator.LE, "<=");
		charOperator.put(ComparisonOperator.GE, ">=");
		
//		constantOperator.put(Operator.EQ, CompareFilter.CompareOp.EQUAL);
//		constantOperator.put(Operator.NE, CompareFilter.CompareOp.NOT_EQUAL);
//		constantOperator.put(Operator.LT, CompareFilter.CompareOp.LESS);
//		constantOperator.put(Operator.GT, CompareFilter.CompareOp.GREATER);
//		constantOperator.put(Operator.LE, CompareFilter.CompareOp.LESS_OR_EQUAL);
//		constantOperator.put(Operator.GE, CompareFilter.CompareOp.GREATER_OR_EQUAL);
	}
	
	
	public static String mapConditionalOperator(ComparisonOperator operator){
		switch (NoSQLMiddleware.getUsedDatabase()) {
		case Cassandra:
			return wordOperators.get(operator);
		case DynamoDb:
			return wordOperators.get(operator);
		case Hbase:
//			return constantOperator.get(operator);
		case Hypertable:
			return charOperator.get(operator);
		}
		
		return "";
	}
//	public static CompareFilter.CompareOp mapConditionalOperatorHBase(Operator operator) {
//		return constantOperator.get(operator);
//	}
}
