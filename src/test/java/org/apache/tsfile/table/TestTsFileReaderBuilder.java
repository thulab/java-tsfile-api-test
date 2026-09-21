package org.apache.tsfile.table;

import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.util.TestDataUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.tsfile.exception.NotCompatibleTsFileException;

import java.io.File;
import java.nio.file.Files;

/** 覆盖 {@link TsFileReaderBuilder} 的参数校验：不存在文件 / 目录 / 非法格式文件。 */
public class TestTsFileReaderBuilder {

  private final File valid =
      FSFactoryProducer.getFSFactory().getFile("data/tsfile/reader_builder_valid.tsfile");
  private final File garbage =
      FSFactoryProducer.getFSFactory().getFile("data/tsfile/reader_builder_garbage.tsfile");

  @BeforeClass
  public void generate() throws Exception {
    TestDataUtils.writeStandardTsFile(valid, 3);
    // 一个内容非法的“伪 tsfile”，用于触发 build 时的格式解析异常
    Files.write(garbage.toPath(), "not a tsfile".getBytes());
  }

  @AfterClass
  public void cleanup() {
    valid.delete();
    garbage.delete();
  }

// 用例 JAVA-TABLE-038：空文件路径Reader异常
  @Test
  public void testNullFileThrows() {
    IllegalArgumentException e =
        Assert.expectThrows(IllegalArgumentException.class, () -> new TsFileReaderBuilder().file(null).build());
    Assert.assertEquals(e.getMessage(), "The file must be a non-null and non-directory File.");
  }

// 用例 JAVA-TABLE-039：不存在文件Reader异常
  @Test
  public void testNonExistFileThrows() {
    File notExist = FSFactoryProducer.getFSFactory().getFile("data/tsfile/definitely_not_exist.tsfile");
    IllegalArgumentException e =
        Assert.expectThrows(IllegalArgumentException.class, () -> new TsFileReaderBuilder().file(notExist).build());
    Assert.assertEquals(e.getMessage(), "The file must be a non-null and non-directory File.");
  }

// 用例 JAVA-TABLE-040：目录路径Reader异常
  @Test
  public void testDirectoryThrows() {
    File dir = FSFactoryProducer.getFSFactory().getFile("data/tsfile");
    IllegalArgumentException e =
        Assert.expectThrows(IllegalArgumentException.class, () -> new TsFileReaderBuilder().file(dir).build());
    Assert.assertEquals(e.getMessage(), "The file must be a non-null and non-directory File.");
  }

  /** 非法格式文件：build 时读文件尾元数据应抛 NotCompatibleTsFileException（TsFileRuntimeException 子类）。 */
// 用例 JAVA-TABLE-041：损坏文件Reader异常
  @Test
  public void testGarbageFileThrows() {
    Assert.expectThrows(NotCompatibleTsFileException.class, () -> new TsFileReaderBuilder().file(garbage).build());
  }
}
