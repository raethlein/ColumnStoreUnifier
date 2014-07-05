import middleware.NoSQLMiddleware.Implementations;

import org.junit.Before;

public class HbaseTest extends ImplementationBaseTest {
	@Before
	public void setUp(){
		TestHandler.initTest(Implementations.Hbase, "0", "0");
	}
}