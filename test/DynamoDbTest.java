import middleware.NoSQLMiddleware.Implementations;

import org.junit.Before;


public class DynamoDbTest extends ImplementationBaseTest{
	@Before
	public void setUp(){
		TestHandler.initTest(Implementations.DynamoDb, "http://localhost", "8000");
	}
}
