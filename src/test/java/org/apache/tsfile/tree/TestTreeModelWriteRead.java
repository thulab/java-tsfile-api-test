package org.apache.tsfile.tree;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** 树模型写读：registerTimeseries + writeTree 分批 flush + writeRecord，再用 QueryExpression 读回校验。 */
public class TestTreeModelWriteRead {

  private final File f = FSFactoryProducer.getFSFactory().getFile("data/tsfile/tree_write_read.tsfile");
  private static final String DEVICE = "root.db1.d1";

  @AfterMethod
  public void cleanup() {
    if (f.exists()) {
      f.delete();
    }
  }

  private List<IMeasurementSchema> schemas() {
    List<IMeasurementSchema> list = new ArrayList<>();
    list.add(new MeasurementSchema("m1", TSDataType.BOOLEAN, TSEncoding.PLAIN));
    list.add(new MeasurementSchema("m2", TSDataType.INT32, TSEncoding.PLAIN));
    list.add(new MeasurementSchema("m3", TSDataType.INT64, TSEncoding.PLAIN));
    list.add(new MeasurementSchema("m4", TSDataType.FLOAT, TSEncoding.PLAIN));
    list.add(new MeasurementSchema("m5", TSDataType.DOUBLE, TSEncoding.PLAIN));
    return list;
  }

  /** writeTree 分批 flush 写入，读回逐行校验时间戳与抽样列值。 */
  @Test
  public void testWriteTreeAndRead() throws Exception {
    if (f.exists()) {
      f.delete();
    }
    int rowNum = 10;
    List<IMeasurementSchema> schemas = schemas();
    try (TsFileWriter writer = new TsFileWriter(f)) {
      for (IMeasurementSchema s : schemas) {
        writer.registerTimeseries(DEVICE, s);
      }
      Tablet tablet = new Tablet(DEVICE, schemas, /* maxRowNumber= */ 4);
      for (int r = 0; r < rowNum; r++) {
        int row = tablet.getRowSize();
        tablet.addTimestamp(row, r);
        tablet.addValue("m1", row, r % 2 == 0);
        tablet.addValue("m2", row, r);
        tablet.addValue("m3", row, (long) r * 100);
        tablet.addValue("m4", row, r + 0.5f);
        tablet.addValue("m5", row, r + 0.25d);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          writer.writeTree(tablet);
          tablet.reset();
        }
      }
      if (tablet.getRowSize() != 0) {
        writer.writeTree(tablet);
      }
    }

    int count = 0;
    try (TsFileSequenceReader seq = new TsFileSequenceReader(f.getPath());
        TsFileReader reader = new TsFileReader(seq)) {
      List<Path> paths = new ArrayList<>();
      paths.add(new Path(DEVICE, "m2", true));
      paths.add(new Path(DEVICE, "m3", true));
      QueryDataSet ds = reader.query(QueryExpression.create(paths, null));
      while (ds.hasNext()) {
        RowRecord rec = ds.next();
        long t = rec.getTimestamp();
        // m2(INT32)=t, m3(INT64)=t*100
        Assert.assertEquals(rec.getFields().get(0).getIntV(), (int) t, "m2 @t=" + t);
        Assert.assertEquals(rec.getFields().get(1).getLongV(), t * 100, "m3 @t=" + t);
        count++;
      }
    }
    Assert.assertEquals(count, rowNum, "树模型读回行数");
  }

  /** writeRecord 单行写入，读回校验。 */
  @Test
  public void testWriteRecordAndRead() throws Exception {
    if (f.exists()) {
      f.delete();
    }
    List<IMeasurementSchema> schemas = schemas();
    try (TsFileWriter writer = new TsFileWriter(f)) {
      for (IMeasurementSchema s : schemas) {
        writer.registerTimeseries(DEVICE, s);
      }
      for (int t = 0; t < 5; t++) {
        TSRecord record = new TSRecord(DEVICE, t);
        record.addPoint("m2", t);
        record.addPoint("m3", (long) t * 100);
        writer.writeRecord(record);
      }
    }

    int count = 0;
    try (TsFileSequenceReader seq = new TsFileSequenceReader(f.getPath());
        TsFileReader reader = new TsFileReader(seq)) {
      QueryDataSet ds =
          reader.query(
              QueryExpression.create(Collections.singletonList(new Path(DEVICE, "m2", true)), null));
      while (ds.hasNext()) {
        RowRecord rec = ds.next();
        Assert.assertEquals(rec.getFields().get(0).getIntV(), (int) rec.getTimestamp());
        count++;
      }
    }
    Assert.assertEquals(count, 5, "writeRecord 读回行数");
  }

  /** registerAlignedTimeseries + writeAligned 对齐写入路径。 */
  @Test
  public void testWriteAlignedAndRead() throws Exception {
    if (f.exists()) {
      f.delete();
    }
    String device = "root.db1.aligned";
    List<IMeasurementSchema> schemas = schemas();
    try (TsFileWriter writer = new TsFileWriter(f)) {
      writer.registerAlignedTimeseries(device, schemas);
      Tablet tablet = new Tablet(device, schemas);
      for (int r = 0; r < 6; r++) {
        tablet.addTimestamp(r, r);
        tablet.addValue("m2", r, r);
        tablet.addValue("m3", r, (long) r * 100);
      }
      writer.writeAligned(tablet);
    }

    int count = 0;
    try (TsFileSequenceReader seq = new TsFileSequenceReader(f.getPath());
        TsFileReader reader = new TsFileReader(seq)) {
      QueryDataSet ds =
          reader.query(
              QueryExpression.create(Collections.singletonList(new Path(device, "m2", true)), null));
      while (ds.hasNext()) {
        ds.next();
        count++;
      }
    }
    Assert.assertEquals(count, 6, "writeAligned 读回行数");
  }
}
