package org.apache.tsfile.table;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.util.TestDataUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * 覆盖 {@link Tablet} 未被现有测试用到的接口：按列索引 addValue、类型不匹配异常、maxRowNumber、
 * reset/getRowSize/getMaxRowNumber、serialize/deserialize 往返等。
 */
public class TestTabletApi {

  private final File f = FSFactoryProducer.getFSFactory().getFile("data/tsfile/tablet_api.tsfile");

  @AfterClass
  public void cleanup() {
    if (f.exists()) {
      f.delete();
    }
  }

  /** 按列索引 addValue 全类型写入，再读回逐值校验（用 Time 作 seed，避免 device 排序问题）。 */
  @Test
  public void testAddValueByColumnIndex() throws Exception {
    if (f.exists()) {
      f.delete();
    }
    int rowNum = 5;
    List<String> names = TestDataUtils.COLUMN_NAMES;
    List<TSDataType> types = TestDataUtils.DATA_TYPES;
    try (ITsFileWriter writer =
        new TsFileWriterBuilder().file(f).tableSchema(TestDataUtils.buildTableSchema()).build()) {
      Tablet tablet = new Tablet(names, types);
      for (int i = 0; i < rowNum; i++) {
        int seed = i + 1;
        tablet.addTimestamp(i, seed);
        for (int col = 0; col < names.size(); col++) {
          Object v = TestDataUtils.expectedValue(col, seed);
          switch (types.get(col)) {
            case STRING:
            case TEXT:
              tablet.addValue(i, col, (String) v);
              break;
            case INT32:
              tablet.addValue(i, col, (int) (Integer) v);
              break;
            case BOOLEAN:
              tablet.addValue(i, col, (boolean) (Boolean) v);
              break;
            case INT64:
            case TIMESTAMP:
              tablet.addValue(i, col, (long) (Long) v);
              break;
            case FLOAT:
              tablet.addValue(i, col, (float) (Float) v);
              break;
            case DOUBLE:
              tablet.addValue(i, col, (double) (Double) v);
              break;
            case BLOB:
              tablet.addValue(i, col, (byte[]) v);
              break;
            case DATE:
              tablet.addValue(i, col, (java.time.LocalDate) v);
              break;
            default:
              throw new IllegalArgumentException("unsupported");
          }
        }
      }
      Assert.assertEquals(tablet.getRowSize(), rowNum, "addTimestamp 应自动维护 rowSize");
      writer.write(tablet);
    }

    int actual = 0;
    try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build();
        ResultSet rs =
            reader.query(TestDataUtils.TABLE_NAME, names, Long.MIN_VALUE, Long.MAX_VALUE)) {
      while (rs.next()) {
        int seed = (int) rs.getLong("Time");
        Assert.assertEquals(rs.getInt("S1"), seed);
        Assert.assertEquals(rs.getString("Tag1"), "Tag1_Value_" + seed);
        actual++;
      }
    }
    Assert.assertEquals(actual, rowNum);
  }

  /**
   * 按列索引 addValue 类型不匹配。列索引 5 = S4 = FLOAT，用 long 重载写入：long 重载先校验目标列是否为
   * long 数组，发现是 float 数组即报错，文案给出 long 重载期望的类型串 INT64/TIMESTAMP（源码逻辑）。
   */
  @Test
  public void testAddValueWrongTypeThrows() {
    Tablet tablet = new Tablet(TestDataUtils.COLUMN_NAMES, TestDataUtils.DATA_TYPES);
    tablet.addTimestamp(0, 1);
    IllegalArgumentException e =
        Assert.expectThrows(IllegalArgumentException.class, () -> tablet.addValue(0, 5, 1L));
    Assert.assertEquals(e.getMessage(), "The data type of column index 5 is not INT64/TIMESTAMP");
  }

  /** maxRowNumber 构造 + reset 语义。 */
  @Test
  public void testMaxRowNumberAndReset() {
    Tablet tablet = new Tablet(TestDataUtils.COLUMN_NAMES, TestDataUtils.DATA_TYPES, 8);
    Assert.assertEquals(tablet.getMaxRowNumber(), 8);
    for (int i = 0; i < 3; i++) {
      tablet.addTimestamp(i, i + 1);
      tablet.addValue(i, "S1", i + 1);
    }
    Assert.assertEquals(tablet.getRowSize(), 3);
    tablet.reset();
    Assert.assertEquals(tablet.getRowSize(), 0, "reset 后 rowSize 归零");
  }

  /** serialize / deserialize 往返：反序列化后行数、时间戳、各列值数组应与原始一致（语义校验）。 */
  @Test
  public void testSerializeDeserializeRoundTrip() throws Exception {
    // serialize 依赖 columnCategories，故用带 category 的构造（Tablet(List,List) 简构造不初始化 categories，无法序列化）
    Tablet tablet =
        new Tablet(
            "t_ser",
            Arrays.asList("Tag1", "S1"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT32),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
    int rowNum = 4;
    for (int i = 0; i < rowNum; i++) {
      tablet.addTimestamp(i, i + 1);
      tablet.addValue(i, "Tag1", "dev_" + (i + 1));
      tablet.addValue(i, "S1", (i + 1) * 10);
    }
    // serialize() 用 ByteBuffer.wrap(buf, 0, size) 返回，已是可读状态（position=0, limit=size），无需 flip
    ByteBuffer buffer = tablet.serialize();
    Tablet restored = Tablet.deserialize(buffer);

    Assert.assertEquals(restored.getRowSize(), rowNum, "往返后行数一致");
    for (int i = 0; i < rowNum; i++) {
      Assert.assertEquals(restored.getTimestamp(i), tablet.getTimestamp(i), "第 " + i + " 行时间戳");
    }
    int[] originS1 = (int[]) tablet.getValues()[1];
    int[] restoredS1 = (int[]) restored.getValues()[1];
    for (int i = 0; i < rowNum; i++) {
      Assert.assertEquals(restoredS1[i], originS1[i], "第 " + i + " 行 S1");
    }
  }
}
