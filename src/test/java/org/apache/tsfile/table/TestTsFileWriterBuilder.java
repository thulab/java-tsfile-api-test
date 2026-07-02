package org.apache.tsfile.table;

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

import static org.apache.tsfile.util.TestDataUtils.COLUMN_NAMES;
import static org.apache.tsfile.util.TestDataUtils.DATA_TYPES;
import static org.apache.tsfile.util.TestDataUtils.TABLE_NAME;

/** 覆盖 {@link TsFileWriterBuilder} 的参数校验异常，以及 memoryThreshold 触发多次 flush 后仍能正确读回。 */
public class TestTsFileWriterBuilder {

  private final File f =
      FSFactoryProducer.getFSFactory().getFile("data/tsfile/writer_builder.tsfile");

  @AfterClass
  public void cleanup() {
    if (f.exists()) {
      f.delete();
    }
  }

  @Test
  public void testNullFileThrows() {
    IllegalArgumentException e =
        Assert.expectThrows(
            IllegalArgumentException.class,
            () -> new TsFileWriterBuilder().file(null).tableSchema(TestDataUtils.buildTableSchema()).build());
    Assert.assertEquals(e.getMessage(), "The file must be a non-null and non-directory File.");
  }

  @Test
  public void testDirectoryFileThrows() {
    File dir = FSFactoryProducer.getFSFactory().getFile("data/tsfile");
    IllegalArgumentException e =
        Assert.expectThrows(
            IllegalArgumentException.class,
            () -> new TsFileWriterBuilder().file(dir).tableSchema(TestDataUtils.buildTableSchema()).build());
    Assert.assertEquals(e.getMessage(), "The file must be a non-null and non-directory File.");
  }

  @Test
  public void testNullSchemaThrows() {
    IllegalArgumentException e =
        Assert.expectThrows(
            IllegalArgumentException.class,
            () -> new TsFileWriterBuilder().file(f).tableSchema(null).build());
    Assert.assertEquals(e.getMessage(), "TableSchema must not be null.");
  }

  @Test
  public void testNonPositiveMemoryThresholdThrows() {
    IllegalArgumentException zero =
        Assert.expectThrows(
            IllegalArgumentException.class,
            () ->
                new TsFileWriterBuilder()
                    .file(f)
                    .tableSchema(TestDataUtils.buildTableSchema())
                    .memoryThreshold(0)
                    .build());
    Assert.assertEquals(zero.getMessage(), "Memory threshold must be > 0 bytes.");

    IllegalArgumentException neg =
        Assert.expectThrows(
            IllegalArgumentException.class,
            () ->
                new TsFileWriterBuilder()
                    .file(f)
                    .tableSchema(TestDataUtils.buildTableSchema())
                    .memoryThreshold(-1)
                    .build());
    Assert.assertEquals(neg.getMessage(), "Memory threshold must be > 0 bytes.");
  }

  /** 极小 memoryThreshold 触发多次 flush；写入 100 行后应能完整读回。 */
  @Test
  public void testSmallMemoryThresholdStillReadsBack() throws Exception {
    if (f.exists()) {
      f.delete();
    }
    int rowNum = 100;
    try (ITsFileWriter writer =
        new TsFileWriterBuilder()
            .file(f)
            .tableSchema(TestDataUtils.buildTableSchema())
            .memoryThreshold(1024) // 极小阈值，强制频繁 flush
            .build()) {
      Tablet tablet = new Tablet(COLUMN_NAMES, DATA_TYPES);
      for (int i = 0; i < rowNum; i++) {
        TestDataUtils.fillRow(tablet, i, i + 1);
      }
      writer.write(tablet);
    }

    int actual = 0;
    try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build();
        ResultSet rs = reader.query(TABLE_NAME, COLUMN_NAMES, Long.MIN_VALUE, Long.MAX_VALUE)) {
      while (rs.next()) {
        // 以 Time 作 seed，与返回顺序无关（不同 TAG=不同 device，非 Time 序）
        int seed = (int) rs.getLong("Time");
        Assert.assertEquals(rs.getInt("S1"), seed); // 抽样校验一列：S1(INT32)=seed
        actual++;
      }
    }
    Assert.assertEquals(actual, rowNum, "多次 flush 后读回行数");
  }
}
