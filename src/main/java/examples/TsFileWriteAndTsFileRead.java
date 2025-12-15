package examples;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * An example of writing data with Tablet to TsFile
 */
public class TsFileWriteAndTsFileRead {


    private static final String path = "data/tsfile/tree.tsfile";
    private static final String database = "root.db1";
    private static final String deviceId = database + ".d1";

    public static void main(String[] args) throws IOException, WriteProcessException {

        File f = FSFactoryProducer.getFSFactory().getFile(path);
        if (f.exists()) {
            Files.delete(f.toPath());
        }

        try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
            List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
            measurementSchemas.add(new MeasurementSchema("m1", TSDataType.BOOLEAN, TSEncoding.PLAIN));
            measurementSchemas.add(new MeasurementSchema("m2", TSDataType.INT32, TSEncoding.PLAIN));
            measurementSchemas.add(new MeasurementSchema("m3", TSDataType.INT64, TSEncoding.PLAIN));
            measurementSchemas.add(new MeasurementSchema("m4", TSDataType.FLOAT, TSEncoding.PLAIN));
            measurementSchemas.add(new MeasurementSchema("m5", TSDataType.DOUBLE, TSEncoding.CAMEL));
            measurementSchemas.add(new MeasurementSchema("m6", TSDataType.TEXT, TSEncoding.PLAIN));
            measurementSchemas.add(new MeasurementSchema("m7", TSDataType.STRING, TSEncoding.PLAIN));
            measurementSchemas.add(new MeasurementSchema("m8", TSDataType.BLOB, TSEncoding.PLAIN));
            measurementSchemas.add(new MeasurementSchema("m9", TSDataType.DATE, TSEncoding.PLAIN));
            measurementSchemas.add(new MeasurementSchema("m10", TSDataType.TIMESTAMP, TSEncoding.PLAIN));

            // register nonAligned timeseries
            for (IMeasurementSchema measurementSchema : measurementSchemas) {
                tsFileWriter.registerTimeseries(deviceId, measurementSchema);
            }

            // example 1
            writeWithTablet(tsFileWriter, deviceId, measurementSchemas, 10, 0);
        }
        readWithTablet();

    }

    private static void writeWithTablet(
            TsFileWriter tsFileWriter,
            String deviceId,
            List<IMeasurementSchema> schemas,
            long rowNum,
            long startTime)
            throws IOException, WriteProcessException {
        Tablet tablet = new Tablet(deviceId, schemas);

        for (int r = 0; r < rowNum; r++) {
            int row = tablet.getRowSize();
            tablet.addTimestamp(row, startTime++);
            if (r % 2 == 0) {
                tablet.addValue("m1", r, r % 3 != 0);
                tablet.addValue("m2", r, r % 3 != 0 ? r * 100 : r * -100);
                tablet.addValue("m3", r, r % 3 != 0 ? r * 100L : r * -100L);
                tablet.addValue("m4", r, r % 3 != 0 ? r * 12345.12345F : r * -12345.12345F);
                tablet.addValue("m5", r, r % 3 != 0 ? r * 12345.12345 : r * -12345.12345);
            } else {
                tablet.addValue("m6", r, String.valueOf(r));
                tablet.addValue("m7", r, String.valueOf(r));
                tablet.addValue("m8", r, new Binary(String.valueOf(r).getBytes(StandardCharsets.UTF_8)));
                tablet.addValue("m9", r, LocalDate.ofEpochDay(r));
                tablet.addValue("m10", r, r % 3 != 0 ? r * 10000L : r * -10000L);
            }
            // write
            if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
                tsFileWriter.writeTree(tablet);
                tablet.reset();
            }
        }
        // write
        if (tablet.getRowSize() != 0) {
            tsFileWriter.writeTree(tablet);
            tablet.reset();
        }
    }

    private static void readWithTablet() throws IOException {
        try (TsFileSequenceReader reader = new TsFileSequenceReader(path);
             TsFileReader readTsFile = new TsFileReader(reader)) {

            // use these paths(all measurements) for all the queries
            ArrayList<Path> paths = new ArrayList<>();
            paths.add(new Path(deviceId, "m1", true));
            paths.add(new Path(deviceId, "m2", true));
            paths.add(new Path(deviceId, "m3", true));
            paths.add(new Path(deviceId, "m4", true));
            paths.add(new Path(deviceId, "m5", true));
            paths.add(new Path(deviceId, "m6", true));
            paths.add(new Path(deviceId, "m7", true));
            paths.add(new Path(deviceId, "m8", true));
            paths.add(new Path(deviceId, "m9", true));
            paths.add(new Path(deviceId, "m10", true));

            QueryExpression queryExpression = QueryExpression.create(paths, null);
            QueryDataSet queryDataSet = readTsFile.query(queryExpression);
            while (queryDataSet.hasNext()) {
                System.out.println(queryDataSet.next().toString());
            }
        }
    }
}