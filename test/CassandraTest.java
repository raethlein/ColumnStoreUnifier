import middleware.NoSQLMiddleware.Implementations;

import org.junit.Before;

public class CassandraTest extends ImplementationBaseTest{
	@Before
	public void setUp(){
		TestHandler.initTest(Implementations.Cassandra, "127.0.0.1", "");
	}
}
