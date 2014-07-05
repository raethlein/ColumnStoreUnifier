package middleware;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class ComparisonOperatorMapper {
	public enum Operator {
		EQ,
		NE,
		LT,
		GT,
		LE,
		GE
	}
	
	private static Map<Operator, String> wordOperators = new HashMap<>();
	private static Map<Operator, String> charOperator = new HashMap<>();
	private static Map<Operator, CompareFilter.CompareOp> constantOperator = new HashMap<>();
	
	public static void initConditionalOperatorMapper(){
		wordOperators.put(Operator.EQ, "EQ");
		wordOperators.put(Operator.NE, "NE");
		wordOperators.put(Operator.LT, "LT");
		wordOperators.put(Operator.GT, "GT");
		wordOperators.put(Operator.LE, "LE");
		wordOperators.put(Operator.GE, "GE");
		
		charOperator.put(Operator.EQ, "=");
		charOperator.put(Operator.NE, "!=");
		charOperator.put(Operator.LT, "<");
		charOperator.put(Operator.GT, ">");
		charOperator.put(Operator.LE, "<=");
		charOperator.put(Operator.GE, ">=");
		
//		constantOperator.put(Operator.EQ, CompareFilter.CompareOp.EQUAL);
//		constantOperator.put(Operator.NE, CompareFilter.CompareOp.NOT_EQUAL);
//		constantOperator.put(Operator.LT, CompareFilter.CompareOp.LESS);
//		constantOperator.put(Operator.GT, CompareFilter.CompareOp.GREATER);
//		constantOperator.put(Operator.LE, CompareFilter.CompareOp.LESS_OR_EQUAL);
//		constantOperator.put(Operator.GE, CompareFilter.CompareOp.GREATER_OR_EQUAL);
	}
	
	
	public static String mapConditionalOperator(Operator operator){
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
