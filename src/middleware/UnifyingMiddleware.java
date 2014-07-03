package middleware;

public class UnifyingMiddleware {

	private static QueryHandler queryHandler;

	public synchronized static QueryHandler getQueryHandler() {
		if (queryHandler == null) {
			Configurator.init();
			ComparisonOperatorMapper.initConditionalOperatorMapper();
			queryHandler = new QueryHandler();
		}
		
		return queryHandler;
	}
}
