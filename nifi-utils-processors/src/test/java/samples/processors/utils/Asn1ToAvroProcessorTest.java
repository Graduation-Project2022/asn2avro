package samples.processors.utils;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class Asn1ToAvroProcessorTest {
	private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(Asn1ToAvroProcessor.class);
    }

    @Test
    public void testProcessor() {

    }
}
