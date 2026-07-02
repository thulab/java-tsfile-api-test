package org.apache.tsfile.table;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.file.metadata.TableSchema;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Locale;

/** 覆盖 ColumnSchemaBuilder / TableSchema 的校验异常与大小写行为，以及 TSDataType 枚举的判定/转换方法。 */
public class TestSchemaAndEnums {

  // ---------------- ColumnSchemaBuilder ----------------

  @Test
  public void testBuilderMissingNameThrows() {
    IllegalStateException e =
        Assert.expectThrows(
            IllegalStateException.class,
            () -> new ColumnSchemaBuilder().dataType(TSDataType.INT32).build());
    Assert.assertEquals(e.getMessage(), "Column name must be set before building");
  }

  @Test
  public void testBuilderMissingTypeThrows() {
    IllegalStateException e =
        Assert.expectThrows(
            IllegalStateException.class,
            () -> new ColumnSchemaBuilder().name("c1").build());
    Assert.assertEquals(e.getMessage(), "Column data type must be set before building");
  }

  @Test
  public void testBuilderEmptyNameThrows() {
    IllegalArgumentException e =
        Assert.expectThrows(IllegalArgumentException.class, () -> new ColumnSchemaBuilder().name(""));
    Assert.assertEquals(e.getMessage(), "Column name must be a non empty string");
  }

  @Test
  public void testBuilderDefaultCategoryIsField() {
    ColumnSchema cs = new ColumnSchemaBuilder().name("c1").dataType(TSDataType.INT32).build();
    Assert.assertEquals(cs.getColumnCategory(), ColumnCategory.FIELD, "未指定 category 默认应为 FIELD");
  }

  // ---------------- TableSchema ----------------

  @Test
  public void testDuplicateColumnThrows() {
    ColumnSchema c1 = new ColumnSchemaBuilder().name("dup").dataType(TSDataType.INT32).category(ColumnCategory.FIELD).build();
    ColumnSchema c2 = new ColumnSchemaBuilder().name("dup").dataType(TSDataType.INT64).category(ColumnCategory.FIELD).build();
    IllegalArgumentException e =
        Assert.expectThrows(
            IllegalArgumentException.class, () -> new TableSchema("t", Arrays.asList(c1, c2)));
    Assert.assertEquals(e.getMessage(), "Each column name in the table should be unique(case insensitive).");
  }

  @Test
  public void testFindColumnIndexCaseInsensitive() {
    ColumnSchema tag = new ColumnSchemaBuilder().name("Tag1").dataType(TSDataType.STRING).category(ColumnCategory.TAG).build();
    ColumnSchema field = new ColumnSchemaBuilder().name("S1").dataType(TSDataType.INT32).category(ColumnCategory.FIELD).build();
    TableSchema schema = new TableSchema("MyTable", Arrays.asList(tag, field));

    // 表名与列名内部小写化
    Assert.assertEquals(schema.getTableName(), "mytable".toLowerCase(Locale.ROOT));
    Assert.assertEquals(schema.findColumnIndex("tag1"), schema.findColumnIndex("TAG1"), "大小写不敏感应返回同一索引");
    Assert.assertTrue(schema.findColumnIndex("S1") >= 0);
  }

  // ---------------- TSDataType ----------------

  @Test
  public void testGetTsDataTypeInvalidByteThrows() {
    IllegalArgumentException e =
        Assert.expectThrows(IllegalArgumentException.class, () -> TSDataType.getTsDataType((byte) 99));
    Assert.assertTrue(e.getMessage().startsWith("Invalid input:"), "非法字节文案应以 Invalid input: 开头，实际：" + e.getMessage());
  }

  @Test
  public void testTypePredicates() {
    Assert.assertTrue(TSDataType.INT32.isNumeric());
    Assert.assertTrue(TSDataType.DOUBLE.isNumeric());
    Assert.assertFalse(TSDataType.STRING.isNumeric());
    Assert.assertFalse(TSDataType.BOOLEAN.isNumeric());

    Assert.assertTrue(TSDataType.TEXT.isBinary());
    Assert.assertTrue(TSDataType.STRING.isBinary());
    Assert.assertFalse(TSDataType.INT64.isBinary());
  }

  @Test
  public void testSerializeDeserializeRoundTrip() {
    for (TSDataType t : TSDataType.values()) {
      Assert.assertEquals(TSDataType.getTsDataType(t.getType()), t, "byte 往返：" + t);
    }
  }

  @Test
  public void testCastIncompatibleThrows() {
    // 不兼容转换应抛 ClassCastException（例如把 BOOLEAN 值当作 INT32 转换）
    Assert.expectThrows(ClassCastException.class, () -> TSDataType.INT32.castFromSingleValue(TSDataType.BOOLEAN, true));
  }
}
