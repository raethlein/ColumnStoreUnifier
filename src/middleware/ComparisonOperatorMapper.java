package middleware;

import java.util.HashMap;
import java.util.Map;

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
	}
	
	
	public static String mapConditionalOperator(Operator operator){
		switch (Configurator.getUsedDatabase()) {
		case Cassandra:
			return wordOperators.get(operator);
		case DynamoDb:
			return wordOperators.get(operator);
		case Hbase:
		case Hypertable:
			return charOperator.get(operator);
		}
		
		return "";
	}
}
