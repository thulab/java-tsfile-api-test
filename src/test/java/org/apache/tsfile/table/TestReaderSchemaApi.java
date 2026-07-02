package org.apache.tsfile.table;

import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.util.TestDataUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static org.apache.tsfile.util.TestDataUtils.COLUMN_NAMES;
import static org.apache.tsfile.util.TestDataUtils.TABLE_NAME;

/** 覆盖 {@link ITsFileReader#getTableSchemas(String)} 与 {@link ITsFileReader#getAllTableSchema()}。 */
public class TestReaderSchemaApi {

  private final File f =
      FSFactoryProducer.getFSFactory().getFile("data/tsfile/reader_schema.tsfile");

  @BeforeClass
  public void generate() throws Exception {
    TestDataUtils.writeStandardTsFile(f, 3);
  }

  @AfterClass
  public void cleanup() {
    if (f.exists()) {
      f.delete();
    }
  }

  @Test
  public void testGetTableSchemasExisting() throws Exception {
    try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build()) {
      Optional<TableSchema> opt = reader.getTableSchemas(TABLE_NAME);
      Assert.assertTrue(opt.isPresent(), "存在的表应返回非空 Optional");
      TableSchema schema = opt.get();
      // 表名内部小写化存储
      Assert.assertEquals(schema.getTableName(), TABLE_NAME.toLowerCase(Locale.ROOT));
      // 列名与类型（列名同样小写化）
      List<IMeasurementSchema> cols = schema.getColumnSchemas();
      Assert.assertEquals(cols.size(), COLUMN_NAMES.size(), "列数");
      for (int i = 0; i < COLUMN_NAMES.size(); i++) {
        Assert.assertEquals(
            cols.get(i).getMeasurementName(), COLUMN_NAMES.get(i).toLowerCase(Locale.ROOT), "第 " + i + " 列名");
        Assert.assertEquals(cols.get(i).getType(), TestDataUtils.DATA_TYPES.get(i), "第 " + i + " 列类型");
      }
    }
  }

  @Test
  public void testGetTableSchemasNonExisting() throws Exception {
    try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build()) {
      Optional<TableSchema> opt = reader.getTableSchemas("no_such_table");
      Assert.assertFalse(opt.isPresent(), "不存在的表应返回 Optional.empty()");
    }
  }

  @Test
  public void testGetAllTableSchema() throws Exception {
    try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build()) {
      List<TableSchema> all = reader.getAllTableSchema();
      Assert.assertEquals(all.size(), 1, "本文件只有一张表");
      Assert.assertEquals(all.get(0).getTableName(), TABLE_NAME.toLowerCase(Locale.ROOT));
    }
  }
}
