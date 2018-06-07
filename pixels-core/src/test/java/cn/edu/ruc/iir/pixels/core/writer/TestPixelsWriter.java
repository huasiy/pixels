package cn.edu.ruc.iir.pixels.core.writer;

import cn.edu.ruc.iir.pixels.core.PixelsWriter;
import cn.edu.ruc.iir.pixels.core.PixelsWriterImpl;
import cn.edu.ruc.iir.pixels.core.TestParams;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.exception.PixelsWriterException;
import cn.edu.ruc.iir.pixels.core.vector.BytesColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.DoubleColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.TimestampColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;
import java.util.Random;

/**
 * pixels
 *
 * @author guodong
 */
public class TestPixelsWriter {
    @Test
    public void testWriter() {
        String filePath = TestParams.filePath;
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        Random random = new Random();

        // schema: struct<a:int,b:float,c:double,d:timestamp,e:boolean,z:string>
        try {
            FileSystem fs = FileSystem.get(URI.create(filePath), conf);
            TypeDescription schema = TypeDescription.fromString(TestParams.schemaStr);
            VectorizedRowBatch rowBatch = schema.createRowBatch();
            LongColumnVector a = (LongColumnVector) rowBatch.cols[0];              // int
            DoubleColumnVector b = (DoubleColumnVector) rowBatch.cols[1];          // float
            DoubleColumnVector c = (DoubleColumnVector) rowBatch.cols[2];          // double
            TimestampColumnVector d = (TimestampColumnVector) rowBatch.cols[3];    // timestamp
            LongColumnVector e = (LongColumnVector) rowBatch.cols[4];              // boolean
            BytesColumnVector z = (BytesColumnVector) rowBatch.cols[5];            // string

            PixelsWriter pixelsWriter =
                    PixelsWriterImpl.newBuilder()
                            .setSchema(schema)
                            .setPixelStride(TestParams.pixelStride)
                            .setRowGroupSize(TestParams.rowGroupSize)
                            .setFS(fs)
                            .setFilePath(new Path(filePath))
                            .setBlockSize(TestParams.blockSize)
                            .setReplication(TestParams.blockReplication)
                            .setBlockPadding(TestParams.blockPadding)
                            .setEncoding(TestParams.encoding)
                            .setCompressionBlockSize(TestParams.compressionBlockSize)
                            .build();

            long curT = System.currentTimeMillis();
            Timestamp timestamp = new Timestamp(curT);
            for (int i = 0; i < TestParams.rowNum; i++) {
                int row = rowBatch.size++;
                if (random.nextInt(10) > 8)
                {
                    a.isNull[row] = true;
                    b.isNull[row] = true;
                    c.isNull[row] = true;
                    d.isNull[row] = true;
                    e.isNull[row] = true;
                    z.isNull[row] = true;
                }
                else
                {
                    a.vector[row] = i;
                    b.vector[row] = i * 3.1415f;
                    c.vector[row] = i * 3.14159d;
                    d.set(row, timestamp);
                    e.vector[row] = i > 25000 ? 1 : 0;
                    z.setVal(row, String.valueOf(i).getBytes());
                }
                if (rowBatch.size == rowBatch.getMaxSize()) {
                    pixelsWriter.addRowBatch(rowBatch);
                    rowBatch.reset();
                }
            }
            if (rowBatch.size != 0) {
                pixelsWriter.addRowBatch(rowBatch);
                rowBatch.reset();
            }
            pixelsWriter.close();
        } catch (IOException | PixelsWriterException e) {
            e.printStackTrace();
        }
    }
}
