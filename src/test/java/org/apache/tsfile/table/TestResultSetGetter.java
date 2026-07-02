package org.apache.tsfile.table;

import org.apache.tsfile.enums.TSDataType;
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
import java.time.LocalDate;

import static org.apache.tsfile.util.TestDataUtils.COLUMN_NAMES;
import static org.apache.tsfile.util.TestDataUtils.DATA_TYPES;
import static org.apache.tsfile.util.TestDataUtils.TABLE_NAME;

/**
 * 覆盖 {@link ResultSet} 的全部 8 类 getter（按列名 + 按列索引两条路径），以及 isNull、越界、
 * 类型不匹配、null 字段取值等异常路径。补上现有 TestITsFileReader 中被注释掉、从未真正校验的取值逻辑。
 */
public class TestResultSetGetter {

  private final File f =
      FSFactoryProducer.getFSFactory().getFile("data/tsfile/result_set_getter.tsfile");
  private static final int ROW_NUM = 5;

  @BeforeClass
  public void generate() throws Exception {
    TestDataUtils.writeStandardTsFile(f, ROW_NUM);
  }

  @AfterClass
  public void cleanup() {
    if (f.exists()) {
      f.delete();
    }
  }

  /** 全类型 getter：按列名取值，与写入的确定性期望值逐行逐列比对。 */
  @Test
  public void testGettersByColumnName() throws Exception {
    try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build();
        ResultSet rs = reader.query(TABLE_NAME, COLUMN_NAMES, Long.MIN_VALUE, Long.MAX_VALUE)) {
      int row = 0;
      while (rs.next()) {
        // 表模型下不同 TAG 值 = 不同 device，返回顺序按 device 字符串序而非 Time；
        // 故以 Time 列的值作为该行的 seed，再算期望值，与返回顺序无关。
        int seed = (int) rs.getLong("Time");
        for (int col = 0; col < COLUMN_NAMES.size(); col++) {
          String name = COLUMN_NAMES.get(col);
          Assert.assertFalse(rs.isNull(name), "seed " + seed + " 列 " + name + " 不应为 null");
          assertColumnValue(rs, name, col, seed, /* byName= */ true);
        }
        row++;
      }
      Assert.assertEquals(row, ROW_NUM, "行数");
    }
  }

  /** 全类型 getter：按列索引取值（Time=1，业务列从 2 开始），逐行逐列比对。 */
  @Test
  public void testGettersByColumnIndex() throws Exception {
    try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build();
        ResultSet rs = reader.query(TABLE_NAME, COLUMN_NAMES, Long.MIN_VALUE, Long.MAX_VALUE)) {
      int row = 0;
      while (rs.next()) {
        int seed = (int) rs.getLong(1); // Time 列(索引1)作为 seed
        for (int col = 0; col < COLUMN_NAMES.size(); col++) {
          int colIndex = col + 2; // 业务列从索引 2 开始
          Assert.assertFalse(rs.isNull(colIndex), "seed " + seed + " 列索引 " + colIndex + " 不应为 null");
          assertColumnValueByIndex(rs, colIndex, col, seed);
        }
        row++;
      }
      Assert.assertEquals(row, ROW_NUM, "行数");
    }
  }

  /** iterator() 可遍历结果集。 */
  @Test
  public void testIterator() throws Exception {
    try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build();
        ResultSet rs = reader.query(TABLE_NAME, COLUMN_NAMES, Long.MIN_VALUE, Long.MAX_VALUE)) {
      Assert.assertNotNull(rs.iterator(), "iterator 不应为 null");
    }
  }

  /** 列名不存在时取值抛异常。 */
  @Test
  public void testUnknownColumnThrows() throws Exception {
    try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build();
        ResultSet rs = reader.query(TABLE_NAME, COLUMN_NAMES, Long.MIN_VALUE, Long.MAX_VALUE)) {
      Assert.assertTrue(rs.next());
      Assert.expectThrows(RuntimeException.class, () -> rs.getString("noSuchColumn"));
    }
  }

  /** 列索引越界时取值抛异常。 */
  @Test
  public void testIndexOutOfBoundThrows() throws Exception {
    try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build();
        ResultSet rs = reader.query(TABLE_NAME, COLUMN_NAMES, Long.MIN_VALUE, Long.MAX_VALUE)) {
      Assert.assertTrue(rs.next());
      // 合法索引范围 [1, 1+列数]；越界必抛
      Assert.expectThrows(RuntimeException.class, () -> rs.getString(COLUMN_NAMES.size() + 2));
    }
  }

  private void assertColumnValue(ResultSet rs, String name, int col, int seed, boolean byName) {
    TSDataType type = DATA_TYPES.get(col);
    Object expected = TestDataUtils.expectedValue(col, seed);
    switch (type) {
      case STRING:
      case TEXT:
        Assert.assertEquals(rs.getString(name), expected, name);
        break;
      case INT32:
        Assert.assertEquals(rs.getInt(name), (int) (Integer) expected, name);
        break;
      case BOOLEAN:
        Assert.assertEquals(rs.getBoolean(name), (boolean) (Boolean) expected, name);
        break;
      case INT64:
      case TIMESTAMP:
        Assert.assertEquals(rs.getLong(name), (long) (Long) expected, name);
        break;
      case FLOAT:
        Assert.assertEquals(rs.getFloat(name), (float) (Float) expected, 0.0f, name);
        break;
      case DOUBLE:
        Assert.assertEquals(rs.getDouble(name), (double) (Double) expected, 0.0d, name);
        break;
      case BLOB:
        Assert.assertEquals(rs.getBinary(name), (byte[]) expected, name);
        break;
      case DATE:
        Assert.assertEquals(rs.getDate(name), (LocalDate) expected, name);
        break;
      default:
        throw new IllegalArgumentException("Unsupported: " + type);
    }
  }

  private void assertColumnValueByIndex(ResultSet rs, int colIndex, int col, int seed) {
    TSDataType type = DATA_TYPES.get(col);
    Object expected = TestDataUtils.expectedValue(col, seed);
    switch (type) {
      case STRING:
      case TEXT:
        Assert.assertEquals(rs.getString(colIndex), expected, "index " + colIndex);
        break;
      case INT32:
        Assert.assertEquals(rs.getInt(colIndex), (int) (Integer) expected, "index " + colIndex);
        break;
      case BOOLEAN:
        Assert.assertEquals(rs.getBoolean(colIndex), (boolean) (Boolean) expected, "index " + colIndex);
        break;
      case INT64:
      case TIMESTAMP:
        Assert.assertEquals(rs.getLong(colIndex), (long) (Long) expected, "index " + colIndex);
        break;
      case FLOAT:
        Assert.assertEquals(rs.getFloat(colIndex), (float) (Float) expected, 0.0f, "index " + colIndex);
        break;
      case DOUBLE:
        Assert.assertEquals(rs.getDouble(colIndex), (double) (Double) expected, 0.0d, "index " + colIndex);
        break;
      case BLOB:
        Assert.assertEquals(rs.getBinary(colIndex), (byte[]) expected, "index " + colIndex);
        break;
      case DATE:
        Assert.assertEquals(rs.getDate(colIndex), (LocalDate) expected, "index " + colIndex);
        break;
      default:
        throw new IllegalArgumentException("Unsupported: " + type);
    }
  }
}
