package org.apache.tsfile.tree;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Collections;

/** 树模型注册冲突异常：重复注册非对齐时序、对已对齐设备再注册。 */
public class TestTreeModelException {

  private final File f = FSFactoryProducer.getFSFactory().getFile("data/tsfile/tree_exception.tsfile");
  private static final String DEVICE = "root.db1.d1";

  @AfterMethod
  public void cleanup() {
    if (f.exists()) {
      f.delete();
    }
  }

  /** 同一设备下重复注册同名非对齐时序应抛 WriteProcessException。 */
  @Test
  public void testDuplicateRegisterThrows() throws Exception {
    if (f.exists()) {
      f.delete();
    }
    try (TsFileWriter writer = new TsFileWriter(f)) {
      writer.registerTimeseries(DEVICE, new MeasurementSchema("m1", TSDataType.INT32, TSEncoding.PLAIN));
      Assert.expectThrows(
          WriteProcessException.class,
          () -> writer.registerTimeseries(DEVICE, new MeasurementSchema("m1", TSDataType.INT32, TSEncoding.PLAIN)));
    }
  }

  /** 对已注册为对齐时序的设备再注册非对齐时序应抛 WriteProcessException。 */
  @Test
  public void testRegisterNonAlignedOnAlignedDeviceThrows() throws Exception {
    if (f.exists()) {
      f.delete();
    }
    String device = "root.db1.aligned";
    try (TsFileWriter writer = new TsFileWriter(f)) {
      writer.registerAlignedTimeseries(
          device,
          Collections.singletonList(new MeasurementSchema("m1", TSDataType.INT32, TSEncoding.PLAIN)));
      IMeasurementSchema more = new MeasurementSchema("m2", TSDataType.INT32, TSEncoding.PLAIN);
      Assert.expectThrows(
          WriteProcessException.class, () -> writer.registerTimeseries(device, more));
    }
  }
}
