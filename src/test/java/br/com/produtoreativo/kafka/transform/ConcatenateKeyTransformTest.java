package br.com.produtoreativo.kafka.transform;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class ConcatenateKeyTransformTest extends TestCase {
  public ConcatenateKeyTransformTest( String testName ) {
      super( testName );
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
      return new TestSuite( ConcatenateKeyTransformTest.class );
  }

  public void testConcatenateKeyTransform() {
      assertTrue( true );
  }
}
