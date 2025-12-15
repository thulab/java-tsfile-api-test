package org.apache.tsfile.table;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.read.ReadProcessException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.TagFilterBuilder;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.ResultSetMetadata;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import utils.ParserCSV;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TestITsFileReader {

    private final String path = "data/tsfile/table.tsfile";
    private final String tableName = "table1";
    private final File f = FSFactoryProducer.getFSFactory().getFile(path);
    private List<String> columnNameList = new ArrayList<>();
    private List<TSDataType> dataTypeList = new ArrayList<>();
    private final List<ColumnSchema> columnSchemaList = new ArrayList<>();
    private int expectRowNum = 0;
    private TableSchema tableSchema;

    private Iterator<Object[]> getData() throws IOException {
        return new ParserCSV().load("data/csv/table.csv", ',');
    }

    @BeforeTest
    public void GenerateTsFile() throws IOException, WriteProcessException {
        if (f.exists()) {
            Files.delete(f.toPath());
        }
        columnNameList = Arrays.asList(
                "Tag1", "Tag2",
                "S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10");
        dataTypeList = Arrays.asList(
                TSDataType.STRING, TSDataType.STRING,
                TSDataType.INT32, TSDataType.BOOLEAN, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
                TSDataType.TEXT, TSDataType.STRING, TSDataType.BLOB, TSDataType.DATE, TSDataType.TIMESTAMP);
        List<ColumnCategory> columnCategoryList = Arrays.asList(
                ColumnCategory.TAG, ColumnCategory.TAG,
                ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD,
                ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD);
        for (int i = 0; i < columnNameList.size(); i++) {
            columnSchemaList.add(new ColumnSchemaBuilder().name(columnNameList.get(i)).dataType(dataTypeList.get(i)).category(columnCategoryList.get(i)).build());
        }
        tableSchema = new TableSchema(tableName, columnSchemaList);

        try (ITsFileWriter writer =
                     new TsFileWriterBuilder()
                             .file(f)
                             .tableSchema(tableSchema)
                             .build()) {
            Tablet tablet = new Tablet(columnNameList, dataTypeList);
            int rowNum = 0;
            Iterator<Object[]> data = getData();
            while (data.hasNext()) {
                Object[] row = data.next();
                tablet.addTimestamp(rowNum, Long.parseLong(row[0].toString()));
                for (int i = 0; i < columnNameList.size(); i++) {
                    switch (dataTypeList.get(i)) {
                        case TEXT:
                        case STRING:
                            if (!row[i + 1].toString().equals("null")) {
                                tablet.addValue(rowNum, columnNameList.get(i), row[i + 1].toString());
                            }
                            break;
                        case INT32:
                            if (!row[i + 1].toString().equals("null")) {
                                tablet.addValue(rowNum, columnNameList.get(i), Integer.parseInt(row[i + 1].toString()));
                            }
                            break;
                        case BOOLEAN:
                            if (!row[i + 1].toString().equals("null")) {
                                tablet.addValue(rowNum, columnNameList.get(i), Boolean.parseBoolean(row[i + 1].toString()));
                            }
                            break;
                        case INT64:
                        case TIMESTAMP:
                            if (!row[i + 1].toString().equals("null")) {
                                tablet.addValue(rowNum, columnNameList.get(i), Long.parseLong(row[i + 1].toString()));
                            }
                            break;
                        case FLOAT:
                            if (!row[i + 1].toString().equals("null")) {
                                tablet.addValue(rowNum, columnNameList.get(i), Float.parseFloat(row[i + 1].toString()));
                            }
                            break;
                        case DOUBLE:
                            if (!row[i + 1].toString().equals("null")) {
                                tablet.addValue(rowNum, columnNameList.get(i), Double.parseDouble(row[i + 1].toString()));
                            }
                            break;
                        case BLOB:
                            if (!row[i + 1].toString().equals("null")) {
                                tablet.addValue(rowNum, columnNameList.get(i), row[i + 1].toString().getBytes(Charset.defaultCharset()));
                            }
                            break;
                        case DATE:
                            if (!row[i + 1].toString().equals("null")) {
                                tablet.addValue(rowNum, columnNameList.get(i), LocalDate.parse(row[i + 1].toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                            }
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported data type: " + dataTypeList.get(i));
                    }
                }
                rowNum++;
                expectRowNum++;
            }
            writer.write(tablet);
        }
    }

    /**
     * 测试查询接口：query(String tableName, List<String> columnNames, long startTime, long endTime)
     */
    @Test
    public void testQuery1() throws IOException, ReadProcessException, NoTableException, NoMeasurementException {
        int actualRowNum = 0;
        try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build();
             ResultSet resultSet = reader.query(tableName, columnNameList, Long.MIN_VALUE, Long.MAX_VALUE)) {
            ResultSetMetadata metadata = resultSet.getMetadata();
            // 验证 Time 列的元数据
            assert metadata.getColumnName(1).equals("Time");
            assert metadata.getColumnType(1).equals(TSDataType.INT64);
            // 验证其他列的元数据
            for (int i = 0; i < columnNameList.size(); i++) {
                assert metadata.getColumnName(i + 2).equals(columnNameList.get(i));
            }
            for (int i = 0; i < dataTypeList.size(); i++) {
                assert metadata.getColumnType(i + 2).equals(dataTypeList.get(i));
            }
            // 验证数据
            while (resultSet.next()) {
//                StringBuilder stringBuilder = new StringBuilder();
//                stringBuilder.append(resultSet.getLong("Time")).append(" ");
//                for (int i = 0; i < columnNameList.size(); i++) {
//                    switch (dataTypeList.get(i)) {
//                        case BLOB:
//                        case TEXT:
//                        case STRING:
//                            stringBuilder.append(resultSet.isNull(columnNameList.get(i)) ? null : resultSet.getString(columnNameList.get(i))).append(" ");
//                            break;
//                        case INT32:
//                            stringBuilder.append(resultSet.isNull(columnNameList.get(i)) ? null : resultSet.getInt(columnNameList.get(i))).append(" ");
//                            break;
//                        case BOOLEAN:
//                            stringBuilder.append(resultSet.isNull(columnNameList.get(i)) ? null : resultSet.getBoolean(columnNameList.get(i))).append(" ");
//                            break;
//                        case INT64:
//                        case TIMESTAMP:
//                            stringBuilder.append(resultSet.isNull(columnNameList.get(i)) ? null : resultSet.getLong(columnNameList.get(i))).append(" ");
//                            break;
//                        case FLOAT:
//                            stringBuilder.append(resultSet.isNull(columnNameList.get(i)) ? null : resultSet.getFloat(columnNameList.get(i))).append(" ");
//                            break;
//                        case DOUBLE:
//                            stringBuilder.append(resultSet.isNull(columnNameList.get(i)) ? null : resultSet.getDouble(columnNameList.get(i))).append(" ");
//                            break;
//                        case DATE:
//                            stringBuilder.append(resultSet.isNull(columnNameList.get(i)) ? null : resultSet.getDate(columnNameList.get(i))).append(" ");
//                            break;
//                        default:
//                            throw new IllegalArgumentException("Unsupported data type: " + dataTypeList.get(i));
//                    }
//                }
//                System.out.println(stringBuilder);
                actualRowNum++;
            }
            assert actualRowNum == expectRowNum : "Actual row number: " + actualRowNum + ", expected row number: " + expectRowNum;
        }
    }

    /**
     * 测试查询接口：query(String tableName, List<String> columnNames, long startTime, long endTime, Filter tagFilter)
     */
    @Test
    public void testQuery2() throws IOException, ReadProcessException, NoTableException, NoMeasurementException {
        TagFilterBuilder filterBuilder = new TagFilterBuilder(tableSchema);

        // 等于
        queryWithFilter(filterBuilder.eq(columnNameList.get(0), "Tag1_Value_3"), 2);
        queryWithFilter(filterBuilder.eq(columnNameList.get(1), "!@#"), 1);
        queryWithFilter(filterBuilder.eq(columnNameList.get(1), "中国"), 1);
        queryWithFilter(filterBuilder.eq(columnNameList.get(1), "ABC"), 1);
        queryWithFilter(filterBuilder.eq(columnNameList.get(1), "12345"), 1);
        queryWithFilter(filterBuilder.eq(columnNameList.get(1), "    "), 1);

        // 不等于
        queryWithFilter(filterBuilder.neq(columnNameList.get(0), "Tag1_Value_3"), 11);

        // 小于
        queryWithFilter(filterBuilder.lt(columnNameList.get(0), "Tag1_Value_3"), 3);

        // 小于等于
        queryWithFilter(filterBuilder.lteq(columnNameList.get(0), "Tag1_Value_3"), 5);

        // 大于
        queryWithFilter(filterBuilder.gt(columnNameList.get(0), "Tag1_Value_3"), 8);

        // 大于等于
        queryWithFilter(filterBuilder.gteq(columnNameList.get(0), "Tag1_Value_3"), 10);

        // 在两个值范围内
        queryWithFilter(filterBuilder.betweenAnd(columnNameList.get(0), "Tag1_Value_3", "Tag1_Value_5"), 6);

        // 在两个值范围外
        queryWithFilter(filterBuilder.notBetweenAnd(columnNameList.get(0), "Tag1_Value_3", "Tag1_Value_5"), 7);

        // 和
        queryWithFilter(filterBuilder.and(filterBuilder.gteq(columnNameList.get(0), "Tag1_Value_3"), filterBuilder.lteq(columnNameList.get(0), "Tag1_Value_5")), 6);
        queryWithFilter(filterBuilder.and(filterBuilder.gteq(columnNameList.get(0), "Tag1_Value_3"), filterBuilder.lteq(columnNameList.get(1), "Tag2_Value_5")), 7);

        // 或
        queryWithFilter(filterBuilder.or(filterBuilder.eq(columnNameList.get(0), "Tag1_Value_3"), filterBuilder.eq(columnNameList.get(0), "Tag1_Value_5")), 4);
        queryWithFilter(filterBuilder.or(filterBuilder.eq(columnNameList.get(0), "Tag1_Value_3"), filterBuilder.eq(columnNameList.get(1), "Tag2_Value_5")), 3);

        // 否
        queryWithFilter(filterBuilder.not(filterBuilder.eq(columnNameList.get(0), "Tag1_Value_2")), 15);

        // 匹配符合正则表达式的内容
        queryWithFilter(filterBuilder.regExp(columnNameList.get(0), "Tag1_Value_[23]"), 3);

        // 排除符合正则表达式的内容
        queryWithFilter(filterBuilder.notRegExp(columnNameList.get(0), "Tag1_Value_[23]"), 10);

        // 匹配符合通配符的内容
        queryWithFilter(filterBuilder.like(columnNameList.get(0), "%"), 13);

        // 排除符合通配符的内容
        queryWithFilter(filterBuilder.notLike(columnNameList.get(0), "Tag1_Value__"), 0);


    }

    /**
     * 测试查询接口异常情况：query(String tableName, List<String> columnNames, long startTime, long endTime, Filter tagFilter)
     */
    @Test
    public void testQuery2Exception() {
        TagFilterBuilder filterBuilder = new TagFilterBuilder(tableSchema);
        // 1.列名不存在
        String columnName1 = "nonExistColumn";
        try {
            filterBuilder.eq(columnName1, "Tag1_Value_3");
            assert false : "预期报错但是没有报错";
        } catch (IllegalArgumentException e) {
            assert e.getMessage().equals("Column '" + columnName1 + "' is not a tag column") : "实际报错与预期不一致，预期：Column '" + columnName1 + "' is not a tag column，实际：" + e.getMessage();
        }
        String columnName2 = "nonExistColumn";
        try {
            filterBuilder.not(filterBuilder.eq(columnName2, "Tag1_Value_2"));
            assert false : "预期报错但是没有报错";
        } catch (IllegalArgumentException e) {
            assert e.getMessage().equals("Column '" + columnName2 + "' is not a tag column") : "实际报错与预期不一致，预期：Column '" + columnName2 + "' is not a tag column，实际：" + e.getMessage();
        }

        // 2.不是TAG列
        String columnName3 = columnNameList.get(columnNameList.size() - 1);
        try {
            filterBuilder.eq(columnName3, "");
            assert false : "预期报错但是没有报错";
        } catch (IllegalArgumentException e) {
            assert e.getMessage().equals("Column '" + columnName3 + "' is not a tag column") : "实际报错与预期不一致，预期：Column '" + columnName3 + "' is not a tag column，实际：" + e.getMessage();
        }

        // 3. value 或 Pattern 为空
        try {
            filterBuilder.eq(columnNameList.get(0), null);
            assert false : "预期报错但是没有报错";
        } catch (NullPointerException e) {
            assert e.getMessage().equals("constant cannot be null") : "实际报错与预期不一致，预期：constant cannot be null，实际：" + e.getMessage();
        }
        try {
            filterBuilder.betweenAnd(columnNameList.get(0), null, "Tag1_Value_2");
            assert false : "预期报错但是没有报错";
        } catch (NullPointerException e) {
            assert e.getMessage().equals("min cannot be null") : "实际报错与预期不一致，预期：min cannot be null，实际：" + e.getMessage();
        }
        try {
            filterBuilder.betweenAnd(columnNameList.get(0), "Tag1_Value_2", null);
            assert false : "预期报错但是没有报错";
        } catch (NullPointerException e) {
            assert e.getMessage().equals("max cannot be null") : "实际报错与预期不一致，预期：max cannot be null，实际：" + e.getMessage();
        }

        // 4. filter为null
        try {
            filterBuilder.and(null, filterBuilder.eq(columnNameList.get(0), "Tag1_Value_2"));
            assert false : "预期报错但是没有报错";
        } catch (NullPointerException e) {
            assert e.getMessage().equals("left cannot be null") : "实际报错与预期不一致，预期：left cannot be null，实际：" + e.getMessage();
        }
        try {
            filterBuilder.and(filterBuilder.eq(columnNameList.get(0), "Tag1_Value_2"), null);
            assert false : "预期报错但是没有报错";
        } catch (NullPointerException e) {
            assert e.getMessage().equals("right cannot be null") : "实际报错与预期不一致，预期：right cannot be null，实际：" + e.getMessage();
        }
        try {
            filterBuilder.not(null);
            assert false : "预期报错但是没有报错";
        } catch (NullPointerException e) {
            assert e.getMessage().equals("filter cannot be null") : "实际报错与预期不一致，预期：filter cannot be null，实际：" + e.getMessage();
        }


    }

    private void queryWithFilter(Filter filter, int expectRowNum) throws IOException, ReadProcessException, NoTableException, NoMeasurementException {
        int actualRowNum = 0;
        try (ITsFileReader reader = new TsFileReaderBuilder().file(f).build();
             ResultSet resultSet = reader.query(tableName, columnNameList, Long.MIN_VALUE, Long.MAX_VALUE, filter)) {
            ResultSetMetadata metadata = resultSet.getMetadata();
            // 验证 Time 列的元数据
            assert metadata.getColumnName(1).equals("Time");
            assert metadata.getColumnType(1).equals(TSDataType.INT64);
            // 验证其他列的元数据
            for (int i = 0; i < columnNameList.size(); i++) {
                assert metadata.getColumnName(i + 2).equals(columnNameList.get(i));
            }
            for (int i = 0; i < dataTypeList.size(); i++) {
                assert metadata.getColumnType(i + 2).equals(dataTypeList.get(i));
            }
            // 验证数据
            while (resultSet.next()) {
//                StringBuilder stringBuilder = new StringBuilder();
//                stringBuilder.append(resultSet.getLong("Time")).append(" ");
//                for (int i = 0; i < columnNameList.size(); i++) {
//                    switch (dataTypeList.get(i)) {
//                        case BLOB:
//                        case TEXT:
//                        case STRING:
//                            stringBuilder.append(resultSet.isNull(columnNameList.get(i)) ? null : resultSet.getString(columnNameList.get(i))).append(" ");
//                            break;
//                        case INT32:
//                            stringBuilder.append(resultSet.isNull(columnNameList.get(i)) ? null : resultSet.getInt(columnNameList.get(i))).append(" ");
//                            break;
//                        case BOOLEAN:
//                            stringBuilder.append(resultSet.isNull(columnNameList.get(i)) ? null : resultSet.getBoolean(columnNameList.get(i))).append(" ");
//                            break;
//                        case INT64:
//                        case TIMESTAMP:
//                            stringBuilder.append(resultSet.isNull(columnNameList.get(i)) ? null : resultSet.getLong(columnNameList.get(i))).append(" ");
//                            break;
//                        case FLOAT:
//                            stringBuilder.append(resultSet.isNull(columnNameList.get(i)) ? null : resultSet.getFloat(columnNameList.get(i))).append(" ");
//                            break;
//                        case DOUBLE:
//                            stringBuilder.append(resultSet.isNull(columnNameList.get(i)) ? null : resultSet.getDouble(columnNameList.get(i))).append(" ");
//                            break;
//                        case DATE:
//                            stringBuilder.append(resultSet.isNull(columnNameList.get(i)) ? null : resultSet.getDate(columnNameList.get(i))).append(" ");
//                            break;
//                        default:
//                            throw new IllegalArgumentException("Unsupported data type: " + dataTypeList.get(i));
//                    }
//                }
//                System.out.println(stringBuilder);
                actualRowNum++;
            }
            assert actualRowNum == expectRowNum : "Actual row number: " + actualRowNum + ", expected row number: " + expectRowNum;
        }
    }


}
