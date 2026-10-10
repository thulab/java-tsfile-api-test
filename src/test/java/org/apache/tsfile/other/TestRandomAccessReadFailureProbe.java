package org.apache.tsfile.other;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.reader.TsFileInput;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * RN-146 批次 2f —— Java 侧注入式读失败矩阵（tester-only 探针，跨接口对照）。
 *
 * <p>目的：回答「Python/C++ 侧实测到的『读失败被静默吞没（空结果 / partial）』在 Java 接口上是否同样
 * 存在」。Java 侧可注入入口是公开接口 {@link TsFileInput}（表模型 v4 读取器只接受 File、无注入入口），
 * 因此本探针走 {@code TsFileSequenceReader(TsFileInput)} → {@code TsFileReader} 的 tree 读取路径。
 *
 * <p>注入口径与 Python {@code FailingBytesIO} / C++ 探针一致：第 N 次 read 抛 IOException，或已服务累计
 * 字节 ≥ cut 时下一次 read 抛 IOException。本类只做「跨接口取证」，不承载 PLM CASE_ID；方法名不带 test
 * 前缀，避免被 start.sh list-cases 扫进用例清单。
 */
public class TestRandomAccessReadFailureProbe {

  private static final String OUT_DIR =
      System.getenv("RN146_BATCH2F_OUT") != null
          ? System.getenv("RN146_BATCH2F_OUT")
          : Paths.get(System.getProperty("java.io.tmpdir"), "rn146-readfailure-probe").toString();
  private static final String DEVICE = "root.db1.d1";
  private static final int ROWS = 10;

  private static final class Stats {
    int calls = 0;
    long served = 0;
  }

  /** 与 Python/C++ 同口径的注入式 source。 */
  private static final class FailingInput implements TsFileInput {
    private final byte[] data;
    private final String path;
    private final int failAt;
    private final long cutBytes;
    private final Stats stats;
    private long pos = 0;

    FailingInput(byte[] data, String path, int failAt, long cutBytes, Stats stats) {
      this.data = data;
      this.path = path;
      this.failAt = failAt;
      this.cutBytes = cutBytes;
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
    public int read(ByteBuffer dst) throws IOException {
      int n = read(dst, pos);
      if (n > 0) {
        pos += n;
      }
      return n;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
      stats.calls++;
      if (failAt > 0 && stats.calls == failAt) {
        throw new IOException("injected failure at read call " + failAt);
      }
      if (cutBytes >= 0 && stats.served >= cutBytes) {
        throw new IOException("injected stream cut after " + stats.served + " bytes");
      }
      if (dst.remaining() == 0) {
        return 0;
      }
      if (position >= data.length) {
        return -1;
      }
      int n = (int) Math.min(dst.remaining(), data.length - position);
      dst.put(data, (int) position, n);
      stats.served += n;
      return n;
    }

    @Override
    public InputStream wrapAsInputStream() {
      return new ByteArrayInputStream(data);
    }

    @Override
    public void close() {}

    @Override
    public String getFilePath() {
      return path;
    }
  }

  private static final class Outcome {
    int calls = 0;
    int rows = -1;
    String error = null;
  }

  private static Outcome runProbe(
      byte[] bytes, String path, int failAt, long cutBytes, String device) {
    Outcome out = new Outcome();
    Stats stats = new Stats();
    TsFileSequenceReader seq = null;
    TsFileReader reader = null;
    try {
      seq = new TsFileSequenceReader(new FailingInput(bytes, path, failAt, cutBytes, stats));
      reader = new TsFileReader(seq);
      List<Path> paths = new ArrayList<>();
      paths.add(new Path(device, "m2", true));
      paths.add(new Path(device, "m3", true));
      QueryDataSet ds = reader.query(QueryExpression.create(paths, null));
      int rows = 0;
      while (ds.hasNext()) {
        RowRecord rec = ds.next();
        if (rec == null) {
          break;
        }
        rows++;
      }
      out.rows = rows;
    } catch (Throwable t) {
      out.error = t.getClass().getSimpleName() + ": " + t.getMessage();
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (Throwable ignored) {
          // 关闭期异常不覆盖主结果
        }
      }
      if (seq != null) {
        try {
          seq.close();
        } catch (Throwable ignored) {
          // 同上
        }
      }
    }
    out.calls = stats.calls;
    return out;
  }

  private static String verdict(Outcome out, int baselineRows) {
    if (out.error != null) {
      return "ERROR(" + out.error + ")";
    }
    if (out.rows == baselineRows) {
      return "FULL";
    }
    if (out.rows == 0) {
      return "EMPTY(swallowed)";
    }
    return "PARTIAL(swallowed)";
  }

  // 产品写入 API 会抛 WriteProcessException（checked），此处统一 throws Exception；
  // 首轮编译因仅声明 IOException 失败（批次 2f 首跑，测试代码问题，已保留原始输出）。
  private static void writeSample(File f) throws Exception {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("m1", TSDataType.BOOLEAN, TSEncoding.PLAIN));
    schemas.add(new MeasurementSchema("m2", TSDataType.INT32, TSEncoding.PLAIN));
    schemas.add(new MeasurementSchema("m3", TSDataType.INT64, TSEncoding.PLAIN));
    try (TsFileWriter writer = new TsFileWriter(f)) {
      for (IMeasurementSchema s : schemas) {
        writer.registerTimeseries(DEVICE, s);
      }
      Tablet tablet = new Tablet(DEVICE, schemas, /* maxRowNumber= */ 4);
      for (int r = 0; r < ROWS; r++) {
        int row = tablet.getRowSize();
        tablet.addTimestamp(row, r);
        tablet.addValue("m1", row, r % 2 == 0);
        tablet.addValue("m2", row, r);
        tablet.addValue("m3", row, (long) r * 100);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          writer.writeTree(tablet);
          tablet.reset();
        }
      }
      if (tablet.getRowSize() != 0) {
        writer.writeTree(tablet);
      }
    }
  }

  @Test
  public void readFailureMatrixProbe() throws Exception {
    Files.createDirectories(Paths.get(OUT_DIR));
    File sample = new File(OUT_DIR, "simple_tree_java.tsfile");
    writeSample(sample);
    byte[] bytes = Files.readAllBytes(sample.toPath());
    PrintStream report =
        new PrintStream(
            new FileOutputStream(new File(OUT_DIR, "probe-matrix.txt")), true, "UTF-8");
    try {
      Outcome base = runProbe(bytes, sample.getPath(), 0, -1, DEVICE);
      String head =
          String.format(
              "[probe] fixture=%s file_bytes=%d%n[probe] baseline rows=%d reads=%d error=%s",
              sample.getPath(), bytes.length, base.rows, base.calls, String.valueOf(base.error));
      emit(head, report);
      Assert.assertTrue(base.error == null, "baseline must not throw: " + base.error);
      Assert.assertTrue(base.rows > 0, "baseline must return rows");

      Outcome noDevice = runProbe(bytes, sample.getPath(), 0, -1, "root.db1.no_such_device");
      emit(
          String.format(
              "[probe] control no_such_device rows=%d error=%s calls=%d",
              noDevice.rows, String.valueOf(noDevice.error), noDevice.calls),
          report);

      int failAtPoints = 0;
      int swallowed = 0;
      int sweepMax = base.calls + 3;
      emit("[probe] --- fail_at matrix (1.." + sweepMax + ") ---", report);
      for (int n = 1; n <= sweepMax; n++) {
        Outcome out = runProbe(bytes, sample.getPath(), n, -1, DEVICE);
        failAtPoints++;
        String v = verdict(out, base.rows);
        if (out.error == null && out.rows < base.rows) {
          swallowed++;
        }
        emit(
            String.format(
                "[probe] fail_at=%2d calls=%2d rows=%d -> %s", n, out.calls, out.rows, v),
            report);
      }

      int cutPoints = 0;
      emit("[probe] --- cut_bytes sweep (256 step 256) ---", report);
      for (long cut = 256; cut < bytes.length; cut += 256) {
        Outcome out = runProbe(bytes, sample.getPath(), 0, cut, DEVICE);
        cutPoints++;
        String v = verdict(out, base.rows);
        if (out.error == null && out.rows < base.rows) {
          swallowed++;
        }
        emit(
            String.format(
                "[probe] cut=%6d calls=%2d rows=%d -> %s", cut, out.calls, out.rows, v),
            report);
      }

      String summary =
          String.format(
              "[probe] summary baseline_rows=%d baseline_calls=%d fail_at_points=%d cut_points=%d swallowed=%d",
              base.rows, base.calls, failAtPoints, cutPoints, swallowed);
      emit(summary, report);
      emit(
          "[probe] verdict="
              + (swallowed > 0 ? "JAVA_SWALLOWS_READ_FAILURE" : "JAVA_PROPAGATES_READ_FAILURE"),
          report);
      Assert.assertTrue(failAtPoints > 0 && cutPoints > 0, "matrix must be non-empty");
    } finally {
      report.close();
    }
  }

  private static void emit(String line, PrintStream report) {
    System.out.println(line);
    report.println(line);
  }
}
