/*
 * Copyright (c) 2014, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc42;

import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;


public class PreparedStatementTest extends BaseTest4 {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    TestUtil.createTable(con, "timestamptztable", "tstz timestamptz");
    TestUtil.createTable(con, "timetztable", "ttz timetz");
  }

  @Override
  public void tearDown() throws SQLException {
    TestUtil.dropTable(con, "timestamptztable");
    TestUtil.dropTable(con, "timetztable");
    super.tearDown();
  }

  @Test
  public void testTimestampTzSetNull() throws SQLException {
    PreparedStatement pstmt = con.prepareStatement("INSERT INTO timestamptztable (tstz) VALUES (?)");

    // valid: fully qualified type to setNull()
    pstmt.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE);
    pstmt.executeUpdate();

    // valid: fully qualified type to setObject()
    pstmt.setObject(1, null, Types.TIMESTAMP_WITH_TIMEZONE);
    pstmt.executeUpdate();

    pstmt.close();
  }

  @Test
  public void testTimeTzSetNull() throws SQLException {
    PreparedStatement pstmt = con.prepareStatement("INSERT INTO timetztable (ttz) VALUES (?)");

    // valid: fully qualified type to setNull()
    pstmt.setNull(1, Types.TIME_WITH_TIMEZONE);
    pstmt.executeUpdate();

    // valid: fully qualified type to setObject()
    pstmt.setObject(1, null, Types.TIME_WITH_TIMEZONE);
    pstmt.executeUpdate();

    pstmt.close();
  }

  @Test
  public void testLargeMaxRows() throws SQLException {
    assumeExtendedQueryProtocol("Max row hint is not supported in simple protocol execution mode");

    String sql = "select * from generate_series(1, 50)";
    PreparedStatement pstmt = con.prepareStatement(sql);

    // return 3 rows
    int count = 0;
    pstmt.setLargeMaxRows(3);
    ResultSet rs = pstmt.executeQuery();
    while (rs.next()) {
      count++;
    }
    Assert.assertEquals("return only 3 rows", 3, count);
    Assert.assertEquals(3, pstmt.getLargeMaxRows());

    // return 15 rows
    count = 0;
    pstmt.setLargeMaxRows(15);
    rs = pstmt.executeQuery();
    while (rs.next()) {
      count++;
    }
    Assert.assertEquals("return only 15 rows", 15, count);
    Assert.assertEquals(15, pstmt.getLargeMaxRows());

    // return all rows
    count = 0;
    pstmt.setLargeMaxRows(0);
    rs = pstmt.executeQuery();
    while (rs.next()) {
      count++;
    }
    Assert.assertEquals("return all 50 rows", 50, count);
    Assert.assertEquals("0 means unlimited", 0, pstmt.getLargeMaxRows());

    pstmt.setLargeMaxRows(3000000000L);
    Assert.assertEquals("Long values are not supported, it should return Integer.MAX_VALUE",
        Integer.MAX_VALUE, pstmt.getLargeMaxRows());

    rs.close();
    pstmt.close();
  }

}
