package org.apache.tsfile.other;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.reader.TsFileInput;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * RN-146 批次 7：Java 侧跨接口复验（tester-only）。
 *
 * <p>覆盖：① 设备列表 getAllDeviceIds() 注入（设备索引）；② tree QueryDataSet 迭代中途失败
 * （记录已交付行数）；③ 记录「表模型 v4 只接 File、无注入入口」。方法名不带 test 前缀，
 * 避免被 start.sh list-cases 收进用例清单。
 */
public class TestRandomAccessCrossInterfaceProbe {

  private static final String OUT =
      System.getenv("RN146_BATCH7_OUT") != null
          ? System.getenv("RN146_BATCH7_OUT")
          : Paths.get(System.getProperty("java.io.tmpdir"), "rn146-crossif-probe")
              .toString();

  /** 运行时由 buildTreeFixture() 赋值的自建夹具路径（不再指向外部固定路径）。 */
  private static String TREE;
  private static final String DEVICE = "root.ln.wf01.wt01";

  private static final class Stats {
    int calls = 0;
  }

  private static final class FailingInput implements TsFileInput {
    private final byte[] data;
    private final String path;
    private final int failAt;
    private final Stats stats;
    private long pos = 0;

    FailingInput(byte[] data, String path, int failAt, Stats stats) {
      this.data = data;
      this.path = path;
      this.failAt = failAt;
      this.stats = stats;
    }

    @Override
    public long size() {
      return data.length;
    }

    @Override
    public long position() {
      return pos;
    }

    @Override
    public TsFileInput position(long newPosition) {
      pos = newPosition;
      return this;
    }

    @Override
    public int read(ByteBuffer dst) throws java.io.IOException {
      int n = read(dst, pos);
      if (n > 0) {
        pos += n;
      }
      return n;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws java.io.IOException {
      stats.calls++;
      if (failAt > 0 && stats.calls == failAt) {
        throw new java.io.IOException("injected failure at read call " + failAt);
      }
      if (dst.remaining() == 0) {
        return 0;
      }
      if (position >= data.length) {
        return -1;
      }
      int n = (int) Math.min(dst.remaining(), data.length - position);
      dst.put(data, (int) position, n);
      return n;
    }

    @Override
    public InputStream wrapAsInputStream() {
      int start = (int) Math.min(Math.max(pos, 0), data.length);
      return new ByteArrayInputStream(data, start, data.length - start);
    }

    @Override
    public void close() {}

    @Override
    public String getFilePath() {
      return path;
    }
  }

  /**
   * 自建 tree 夹具（不依赖外部路径）：root.ln.wf01.wt01 / temperature(DOUBLE) + status(INT32)。
   */
  private static String buildTreeFixture() throws Exception {
    File dir = new File(OUT);
    Files.createDirectories(dir.toPath());
    File sample = new File(dir, "simple_tree.tsfile");
    if (sample.isFile() && sample.length() > 0) {
      return sample.getPath();
    }
    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("temperature", TSDataType.DOUBLE, TSEncoding.PLAIN));
    schemas.add(new MeasurementSchema("status", TSDataType.INT32, TSEncoding.PLAIN));
    try (TsFileWriter writer = new TsFileWriter(sample)) {
      for (IMeasurementSchema s : schemas) {
        writer.registerTimeseries(DEVICE, s);
      }
      Tablet tablet = new Tablet(DEVICE, schemas, /* maxRowNumber= */ 8);
      for (int r = 0; r < 16; r++) {
        int row = tablet.getRowSize();
        tablet.addTimestamp(row, r);
        tablet.addValue("temperature", row, 20.0 + r);
        tablet.addValue("status", row, r % 2);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          writer.writeTree(tablet);
          tablet.reset();
        }
      }
      if (tablet.getRowSize() != 0) {
        writer.writeTree(tablet);
      }
    }
    return sample.getPath();
  }

  @Test
  public void crossInterfaceProbe() throws Exception {
    Files.createDirectories(Paths.get(OUT));
    TREE = buildTreeFixture();
    PrintStream out =
        new PrintStream(new FileOutputStream(new File(OUT, "java-crossif.txt")), true, "UTF-8");
    byte[] data = Files.readAllBytes(new File(TREE).toPath());

    // 基线：设备列表 + tree 查询行数
    Stats baseStats = new Stats();
    int baseDevices;
    int baseQueryCalls;
    try (TsFileSequenceReader seq =
            new TsFileSequenceReader(new FailingInput(data, TREE, 0, baseStats))) {
      TsFileReader reader = new TsFileReader(seq);
      baseDevices = reader.getAllDeviceIds().size();
      reader.close();
    }
    Stats qStats = new Stats();
    int baseRows;
    try (TsFileSequenceReader seq =
            new TsFileSequenceReader(new FailingInput(data, TREE, 0, qStats))) {
      TsFileReader reader = new TsFileReader(seq);
      QueryDataSet ds = reader.query(queryExpression());
      int rows = 0;
      while (ds.hasNext()) {
        ds.next();
        rows++;
      }
      baseRows = rows;
      baseQueryCalls = qStats.calls;
      reader.close();
    }
    emit(out, String.format(
        "[probe] java baseline devices=%d tree_rows=%d query_reads=%d",
        baseDevices, baseRows, baseQueryCalls));

    // ① 设备列表注入矩阵（设备索引）
    int devSwallow = 0;
    for (int n = 1; n <= baseQueryCalls + 2; n++) {
      int devices = -1;
      String err = null;
      try (TsFileSequenceReader seq =
              new TsFileSequenceReader(new FailingInput(data, TREE, n, new Stats()))) {
        TsFileReader reader = new TsFileReader(seq);
        devices = reader.getAllDeviceIds().size();
        reader.close();
      } catch (Throwable t) {
        err = t.getClass().getSimpleName() + ": " + t.getMessage();
      }
      boolean swallow = err == null && devices >= 0 && devices < baseDevices;
      if (swallow) devSwallow++;
      emit(out, String.format(
          "[probe] java devices fail_at=%2d devices=%d -> %s",
          n, devices, err == null ? (swallow ? "SWALLOW" : "ok") : "ERROR(" + err + ")"));
    }

    // ② tree 查询 + 迭代中途失败（记录已交付行数）
    int iterSwallow = 0;
    for (int n = 1; n <= baseQueryCalls + 2; n++) {
      int delivered = 0;
      String err = null;
      try (TsFileSequenceReader seq =
              new TsFileSequenceReader(new FailingInput(data, TREE, n, new Stats()))) {
        TsFileReader reader = new TsFileReader(seq);
        QueryDataSet ds = reader.query(queryExpression());
        while (ds.hasNext()) {
          ds.next();
          delivered++;
        }
        reader.close();
      } catch (Throwable t) {
        err = t.getClass().getSimpleName() + ": " + t.getMessage();
      }
      boolean swallow = err == null && delivered < baseRows;
      if (swallow) iterSwallow++;
      emit(out, String.format(
          "[probe] java tree fail_at=%2d delivered=%d/%d -> %s",
          n, delivered, baseRows,
          err == null ? (swallow ? "SWALLOW" : "ok") : "ERROR(" + err + ")"));
    }
    emit(out, String.format(
        "[probe] java summary devices_swallow=%d tree_swallow=%d verdict=%s",
        devSwallow, iterSwallow,
        (devSwallow + iterSwallow) > 0 ? "JAVA_SWALLOWS" : "JAVA_OK"));
    emit(out, "[probe] java note: 表模型 v4 读取器（TsFileReaderBuilder）只接受 File，无 TsFileInput 注入入口");
    out.close();
  }

  private static QueryExpression queryExpression() {
    List<Path> paths = new ArrayList<>();
    paths.add(new Path(DEVICE, "temperature", true));
    paths.add(new Path(DEVICE, "status", true));
    return QueryExpression.create(paths, null);
  }

  private static void emit(PrintStream out, String line) {
    System.out.println(line);
    out.println(line);
  }
}
