import static org.junit.Assert.*;

import org.junit.Test;

public class otherTests {

	@Test
	public void stringCompareTest() {
		assertEquals(-1, "42".compareTo("23"));
		//assertEquals("0", "test".compareTo("myTest"));
		assertEquals(1, "1.2".compareTo("1.5"));
	}

}
