package org.apache.tsfile.util;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 测试通用工具：构造覆盖全部数据类型的表 schema / Tablet，并写出标准 tsfile 供读侧测试复用。
 *
 * <p>schema 布局与 {@code TestITsFileReader} 一致：2 个 TAG 列（Tag1、Tag2，STRING）+ 10 个 FIELD 列
 * （S1..S10，覆盖 INT32/BOOLEAN/INT64/FLOAT/DOUBLE/TEXT/STRING/BLOB/DATE/TIMESTAMP）。
 */
public final class TestDataUtils {

  public static final String TABLE_NAME = "table1";

  public static final List<String> COLUMN_NAMES =
      Arrays.asList("Tag1", "Tag2", "S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10");

  public static final List<TSDataType> DATA_TYPES =
      Arrays.asList(
          TSDataType.STRING, TSDataType.STRING,
          TSDataType.INT32, TSDataType.BOOLEAN, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
          TSDataType.TEXT, TSDataType.STRING, TSDataType.BLOB, TSDataType.DATE, TSDataType.TIMESTAMP);

  public static final List<ColumnCategory> COLUMN_CATEGORIES =
      Arrays.asList(
          ColumnCategory.TAG, ColumnCategory.TAG,
          ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD,
          ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD,
          ColumnCategory.FIELD, ColumnCategory.FIELD);

  private TestDataUtils() {}

  /** 构造 12 列（2 TAG + 10 FIELD）覆盖全类型的表 schema。 */
  public static TableSchema buildTableSchema() {
    List<ColumnSchema> columnSchemaList = new ArrayList<>();
    for (int i = 0; i < COLUMN_NAMES.size(); i++) {
      columnSchemaList.add(
          new ColumnSchemaBuilder()
              .name(COLUMN_NAMES.get(i))
              .dataType(DATA_TYPES.get(i))
              .category(COLUMN_CATEGORIES.get(i))
              .build());
    }
    return new TableSchema(TABLE_NAME, columnSchemaList);
  }

  /**
   * 按第 {@code seed} 行为每列填入一个确定性的非空值（用于取值校验，故意不含 null）。
   * 各类型的值由 {@link #expectedValue(int, int)} 定义，读侧可用同一函数算出期望值逐一比对。
   */
  public static void fillRow(Tablet tablet, int rowIndex, int seed) {
    tablet.addTimestamp(rowIndex, seed);
    for (int col = 0; col < COLUMN_NAMES.size(); col++) {
      String name = COLUMN_NAMES.get(col);
      switch (DATA_TYPES.get(col)) {
        case STRING:
        case TEXT:
          tablet.addValue(rowIndex, name, (String) expectedValue(col, seed));
          break;
        case INT32:
          tablet.addValue(rowIndex, name, (int) (Integer) expectedValue(col, seed));
          break;
        case BOOLEAN:
          tablet.addValue(rowIndex, name, (boolean) (Boolean) expectedValue(col, seed));
          break;
        case INT64:
        case TIMESTAMP:
          tablet.addValue(rowIndex, name, (long) (Long) expectedValue(col, seed));
          break;
        case FLOAT:
          tablet.addValue(rowIndex, name, (float) (Float) expectedValue(col, seed));
          break;
        case DOUBLE:
          tablet.addValue(rowIndex, name, (double) (Double) expectedValue(col, seed));
          break;
        case BLOB:
          tablet.addValue(rowIndex, name, (byte[]) expectedValue(col, seed));
          break;
        case DATE:
          tablet.addValue(rowIndex, name, (LocalDate) expectedValue(col, seed));
          break;
        default:
          throw new IllegalArgumentException("Unsupported data type: " + DATA_TYPES.get(col));
      }
    }
  }

  /**
   * 第 {@code col} 列、第 {@code seed} 行应有的确定性期望值（与 {@link #fillRow} 严格对应）。
   * BLOB 返回 {@code byte[]}，读侧用 {@code getBinary} 取回后需按字节数组比较。
   */
  public static Object expectedValue(int col, int seed) {
    switch (col) {
      case 0: // Tag1 STRING
        return "Tag1_Value_" + seed;
      case 1: // Tag2 STRING
        return "Tag2_Value_" + seed;
      case 2: // S1 INT32
        return seed;
      case 3: // S2 BOOLEAN
        return seed % 2 == 0;
      case 4: // S3 INT64
        return (long) seed * 100;
      case 5: // S4 FLOAT
        return seed + 0.5f;
      case 6: // S5 DOUBLE
        return seed + 0.25d;
      case 7: // S6 TEXT
        return "text_" + seed;
      case 8: // S7 STRING
        return "string_" + seed;
      case 9: // S8 BLOB
        return ("blob_" + seed).getBytes(StandardCharsets.UTF_8);
      case 10: // S9 DATE
        return LocalDate.of(2000, 1, 1).plusDays(seed);
      case 11: // S10 TIMESTAMP
        return (long) seed * 1000;
      default:
        throw new IllegalArgumentException("Unexpected column: " + col);
    }
  }

  /**
   * 写出一个含 {@code rowNum} 行、全类型非空数据的标准 tsfile 到 {@code file}（覆盖已存在文件）。
   * 行 i 用 seed=i+1 填充，读侧可用 {@link #expectedValue(int, int)} 以相同 seed 校验。
   */
  public static void writeStandardTsFile(File file, int rowNum) throws Exception {
    if (file.exists()) {
      Files.delete(file.toPath());
    }
    TableSchema schema = buildTableSchema();
    try (ITsFileWriter writer =
        new TsFileWriterBuilder().file(file).tableSchema(schema).build()) {
      Tablet tablet = new Tablet(COLUMN_NAMES, DATA_TYPES);
      for (int i = 0; i < rowNum; i++) {
        // addTimestamp 内部自动维护 rowSize（rowSize = max(rowSize, rowIndex+1)），无需显式 setRowSize
        fillRow(tablet, i, i + 1);
      }
      writer.write(tablet);
    }
  }
}
