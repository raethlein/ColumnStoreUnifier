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
		
		constantOperator.put(ComparisonOperator.EQ, CompareFilter.CompareOp.EQUAL);
		constantOperator.put(ComparisonOperator.NE, CompareFilter.CompareOp.NOT_EQUAL);
		constantOperator.put(ComparisonOperator.LT, CompareFilter.CompareOp.LESS);
		constantOperator.put(ComparisonOperator.GT, CompareFilter.CompareOp.GREATER);
		constantOperator.put(ComparisonOperator.LE, CompareFilter.CompareOp.LESS_OR_EQUAL);
		constantOperator.put(ComparisonOperator.GE, CompareFilter.CompareOp.GREATER_OR_EQUAL);
	}
	
	
	public static Object mapConditionalOperator(ComparisonOperator operator){
		switch (NoSQLMiddleware.getUsedDatabase()) {
		case Cassandra:
			return charOperator.get(operator);
		case DynamoDb:
			return wordOperators.get(operator);
		case Hbase:
			return constantOperator.get(operator);
		case Hypertable:
			return charOperator.get(operator);
		}
		
		return "";
	}
	
	public static String mapStandardizedConditionalOperator(ComparisonOperator operator){
		return charOperator.get(operator);
	}
}
