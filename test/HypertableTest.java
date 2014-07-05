import middleware.NoSQLMiddleware.Implementations;

import org.junit.Before;

public class HypertableTest extends ImplementationBaseTest{
	@Before
	public void setUp(){
		TestHandler.initTest(Implementations.Hypertable, "localhost", "38080");
	}
}
