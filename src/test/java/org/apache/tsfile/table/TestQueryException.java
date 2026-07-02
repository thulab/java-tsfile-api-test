package org.apache.tsfile.table;

import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.util.TestDataUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;

import static org.apache.tsfile.util.TestDataUtils.COLUMN_NAMES;
import static org.apache.tsfile.util.TestDataUtils.TABLE_NAME;

/** 覆盖 query 的异常路径（表名/列名不存在）、时间闭区间语义、以及表名/列名大小写不敏感。 */
public class TestQueryException {

  private final File f =
      FSFactoryProducer.getFSFactory().getFile("data/tsfile/query_exception.tsfile");
  private static final int ROW_NUM = 10;

  @BeforeClass
  public void generate() throws Exception {
    TestDataUtils.writeStandardTsFile(f, ROW_NUM); // Time = 1..10
  }

  @AfterClass
  public void cleanup() {
    if (f.exists()) {
      f.delete();
    }
  }

  @Test
  public void testUnknownTableThrows() throws Exception {
    try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build()) {
      NoTableException e =
          Assert.expectThrows(
              NoTableException.class,
              () -> reader.query("no_such_table", COLUMN_NAMES, Long.MIN_VALUE, Long.MAX_VALUE).close());
      // 表名内部小写化
      Assert.assertEquals(e.getMessage(), "Table no_such_table not found");
    }
  }

  @Test
  public void testUnknownColumnThrows() throws Exception {
    try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build()) {
      NoMeasurementException e =
          Assert.expectThrows(
              NoMeasurementException.class,
              () ->
                  reader
                      .query(TABLE_NAME, Collections.singletonList("no_such_column"), Long.MIN_VALUE, Long.MAX_VALUE)
                      .close());
      Assert.assertEquals(e.getMessage(), "No measurement for no_such_column");
    }
  }

  /** 时间范围为闭区间：start==end==某存在的时间戳，应命中该行。 */
  @Test
  public void testTimeRangeClosedInterval() throws Exception {
    try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build();
        ResultSet rs = reader.query(TABLE_NAME, COLUMN_NAMES, 5, 5)) {
      int count = 0;
      while (rs.next()) {
        Assert.assertEquals(rs.getLong("Time"), 5L, "闭区间 [5,5] 只应命中 Time=5");
        count++;
      }
      Assert.assertEquals(count, 1, "闭区间 [5,5] 命中行数");
    }
  }

  /** 时间窗口 [3,6] 应命中 Time∈{3,4,5,6} 共 4 行。 */
  @Test
  public void testTimeRangeWindow() throws Exception {
    try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build();
        ResultSet rs = reader.query(TABLE_NAME, COLUMN_NAMES, 3, 6)) {
      int count = 0;
      while (rs.next()) {
        long t = rs.getLong("Time");
        Assert.assertTrue(t >= 3 && t <= 6, "命中的 Time 应在 [3,6]，实际 " + t);
        count++;
      }
      Assert.assertEquals(count, 4, "窗口 [3,6] 命中行数");
    }
  }

  /** 表名/列名大小写不敏感：用大写表名和大写列名查询应返回相同结果。 */
  @Test
  public void testCaseInsensitive() throws Exception {
    try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build();
        ResultSet rs =
            reader.query(
                TABLE_NAME.toUpperCase(Locale.ROOT),
                Arrays.asList("TAG1", "S1"),
                Long.MIN_VALUE,
                Long.MAX_VALUE)) {
      int count = 0;
      while (rs.next()) {
        count++;
      }
      Assert.assertEquals(count, ROW_NUM, "大小写不敏感查询应返回全部行");
    }
  }
}
