package middleware;

public class UnifyingMiddleware {

	private static QueryHandler queryHandler;

	public synchronized static QueryHandler getQueryHandler() {
		if (queryHandler == null) {
			Configurator.init();
			queryHandler = new QueryHandler();
		}
		
		return queryHandler;
	}
}
