/*
 * Copyright 2023 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.common.physical.PhysicalWriterUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.exception.PixelsWriterException;
import io.pixelsdb.pixels.core.utils.BlockingMap;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.core.writer.ColumnWriter;
import io.pixelsdb.pixels.core.writer.PixelsWriterOption;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.asynchttpclient.*;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.pixelsdb.pixels.common.utils.Constants.FILE_MAGIC;
import static io.pixelsdb.pixels.core.writer.ColumnWriter.newColumnWriter;
import static java.util.Objects.requireNonNull;

/**
 * PixelsWriterStreamImpl is an implementation of {@link PixelsWriter} that writes
 * ColumnChunks to a stream, for operator pipelining over HTTP.
 *
 * <p>
 * DESIGN:
 * In partitioned mode, each partition worker divides its assigned file list into multiple partitions.
 * Each partition contains several files identified by a partitionId, corresponding to the partition worker's workerId:
 * 0   1   2   3   4
 * Within each partition, data is further hashed based on a hash value that corresponds to the workerId of the
 * next-level workers:
 * ---------------------
 * | 0 | 0 | 0 | 0 | 0 |
 * |---|---|---|---|---|
 * | 1 | 1 | 1 | 1 | 1 |
 * |---|---|---|---|---|
 * | 2 | 2 | 2 | 2 | 2 |
 * |---|---|---|---|---|
 * <p>
 * Each partition worker sends its hashed data parts to the corresponding join workers in sequence. For example:
 *  - Partition worker 0 sends its hash=0 part (of partition 0) to join worker 0, hash=1 part to join worker 1, etc.
 *  - The same pattern is followed by partition workers 1, 2, 3, 4, etc.
 * Each join worker listens on a specific port for all parts with the same hash value across all partitions.
 * Consequently, a partition worker must send each hash part (within its partition) to different ports.
 */
@NotThreadSafe
public class PixelsWriterStreamImpl implements PixelsWriter
{
    private static final Logger LOGGER = LogManager.getLogger(PixelsWriterStreamImpl.class);

    private static final ByteOrder WRITER_ENDIAN;
    /**
     * The number of bytes that the start offset of each column chunk is aligned to.
     */
    private static final int CHUNK_ALIGNMENT;
    /**
     * The byte buffer padded to each column chunk for alignment.
     */
    private static final byte[] CHUNK_PADDING_BUFFER;

    static
    {
        boolean littleEndian = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("column.chunk.little.endian"));
        if (littleEndian)
        {
            WRITER_ENDIAN = ByteOrder.LITTLE_ENDIAN;
        }
        else
        {
            WRITER_ENDIAN = ByteOrder.BIG_ENDIAN;
        }
        CHUNK_ALIGNMENT = Integer.parseInt(ConfigFactory.Instance().getProperty("column.chunk.alignment"));
        checkArgument(CHUNK_ALIGNMENT >= 0, "column.chunk.alignment must >= 0");
        CHUNK_PADDING_BUFFER = new byte[CHUNK_ALIGNMENT];
    }

    /**
     * We use the X-Partition-Id header to pass the partition ID in the HTTP streaming mode.
     * We use -1 to indicate non-partitioned data, and -2 to indicate that this data packet is for passing the schema.
     */
    public static final int PARTITION_ID_SCHEMA_WRITER = -2;

    private final TypeDescription schema;
    private final int rowGroupSize;
    private final PixelsProto.CompressionKind compressionKind;
    private final int compressionBlockSize;
    private final TimeZone timeZone;
    /**
     * The writer option for the column writers.
     */
    private final PixelsWriterOption columnWriterOption;
    private final boolean partitioned;
    // DESIGN: In non-partitioned mode, the writer sends a CLOSE packet to the server to indicate the end of the stream.
    // But since packets could arrive out of order, we do not appoint a specific writer to send the CLOSE packet in
    //  partitioned mode. The HTTP server (reader) should only close the connection when it receives enough packets.
    private final Optional<List<Integer>> partKeyColumnIds;

    private final ColumnWriter[] columnWriters;
    private int fileRowNum;
    private int rowGroupNum = 0;

    private long writtenBytes = 0L;
    private boolean isFirstRowGroup = true;
    private long curRowGroupOffset = 0L;
    private long curRowGroupFooterOffset = 0L;
    private long curRowGroupNumOfRows = 0L;
    private int curRowGroupDataLength = 0;
    /**
     * Whether any current hash value has been set.
     */
    private boolean hashValueIsSet = false;
    private int currHashValue = 0;
    private final int partitionId = 0;

    // output to file
    ByteBuffer tmpFileBuf = ByteBuffer.allocate(10*1024*1024);

    private final ByteBuf byteBuf = Unpooled.buffer();
    /**
     * DESIGN: We only translate fileName to URI when we need to send a row group to the server, rather than at
     *  construction time. This is because the getPort() call is blocking, and so it's better to postpone it as much as
     *  possible.
     * On the other hand, In partitioned mode, we send at most one row group to each upper-level worker (for now), and
     *  so we do not need to translate fileName to URI at construction time.
     */
    private URI uri;
    private final String fileName = "";
    private final List<URI> uris = null;

    private final AsyncHttpClient httpClient = null;
    /**
     * Currently, only 1 outstanding request is allowed, for the sake of simplicity.
     * i.e., the writer will block if there is already an outstanding request, and only sends the next row group
     * after the previous request returns.
     */
    private final Semaphore outstandingHTTPRequestSemaphore = new Semaphore(1);

    private final List<TypeDescription> children;
    private final ExecutorService columnWriterService = Executors.newCachedThreadPool();

    ////////////////////////////////////////////////////////////////////////////
    // deprecated
    private static final BlockingMap<String, Integer> pathToPort = new BlockingMap<>();
    private static final ConcurrentHashMap<String, Integer> pathToSchemaPort = new ConcurrentHashMap<>();
    // We allocate data ports in ascending order, starting from `firstPort`;
    // and allocate schema ports in descending order, starting from `firstPort - 1`.
    private static final int firstPort = 50100;
    private static final AtomicInteger nextPort = new AtomicInteger(firstPort);
    private static final AtomicInteger schemaPorts = new AtomicInteger(firstPort - 1);

    private static int getPort(String path)
    {
        // XXX: Ideally, the getPort() should block until the server started. Otherwise, the server may not be ready
        // when the client tries to connect.
        //  Currently, we resolve this by using Spring Retry in the HTTP client.
        try
        {
            int ret = pathToPort.get(path);
            // ArrayBlockingQueue.take() and .poll() removes element from the queue, so we need to put it back
            setPort(path, ret);
            return ret;
        }
        catch (InterruptedException e)
        {
            LOGGER.error("error when getting port", e);
            return -1;
        }
    }

    private static int getOrSetPort(String path)
    {
        if (pathToPort.exist(path))
        {
            return getPort(path);
        }
        else
        {
            int port = nextPort.getAndIncrement();
            setPort(path, port);
            return port;
        }
    }

    private static void setPort(String path, int port)
    {
        pathToPort.put(path, port);
    }

    public static int getSchemaPort(String path)
    {
        return pathToSchemaPort.computeIfAbsent(path, k -> schemaPorts.getAndDecrement());
    }

    private String fileNameToUri(String fileName)
    {
        return "http://localhost:" + getPort(fileName) + "/";
    }
    // above are deprecated
    ////////////////////////////////////////////////////////////////////////////

    private final PhysicalWriter physicalWriter;

    private PixelsWriterStreamImpl(
            TypeDescription schema,
            int pixelStride,
            int rowGroupSize,
            PixelsProto.CompressionKind compressionKind,
            int compressionBlockSize,
            TimeZone timeZone,
            PhysicalWriter physicalWriter,
            EncodingLevel encodingLevel,
            boolean nullsPadding,
            boolean partitioned,
            Optional<List<Integer>> partKeyColumnIds)
    {
        this.schema = requireNonNull(schema, "schema is null");
        checkArgument(pixelStride > 0, "pixel stripe is not positive");
        checkArgument(rowGroupSize > 0, "row group size is not positive");
        this.rowGroupSize = rowGroupSize;
        this.compressionKind = requireNonNull(compressionKind, "compressionKind is null");
        checkArgument(compressionBlockSize > 0, "compression block size is not positive");
        this.compressionBlockSize = compressionBlockSize;
        this.timeZone = requireNonNull(timeZone);
        this.partitioned = partitioned;
        this.partKeyColumnIds = requireNonNull(partKeyColumnIds, "partKeyColumnIds is null");
        this.children = schema.getChildren();
        checkArgument(!requireNonNull(children, "schema is null").isEmpty(), "schema is empty");
        this.columnWriters = new ColumnWriter[children.size()];
//        this.fileColStatRecorders = new StatsRecorder[children.size()];
        this.columnWriterOption = new PixelsWriterOption()
                .pixelStride(pixelStride)
                .encodingLevel(requireNonNull(encodingLevel, "encodingLevel is null"))
                .byteOrder(WRITER_ENDIAN)
                .nullsPadding(nullsPadding);
        for (int i = 0; i < children.size(); ++i)
        {
            columnWriters[i] = newColumnWriter(children.get(i), columnWriterOption);
//            fileColStatRecorders[i] = StatsRecorder.create(children.get(i));
        }

//        this.rowGroupInfoList = new LinkedList<>();
//        this.rowGroupStatisticList = new LinkedList<>();
        this.physicalWriter = physicalWriter;

        // send header when built?
        try {
            writeHeader();
        } catch (IOException e) {
            throw new PixelsWriterException(
                    "Failed to create PixelsWriter due to error of creating PhysicalWriter");
        }
    }

    public static class Builder
    {
        private TypeDescription builderSchema = null;
        private int builderPixelStride = 0;
        private int builderRowGroupSize = 0;
        private PixelsProto.CompressionKind builderCompressionKind = PixelsProto.CompressionKind.NONE;
        private int builderCompressionBlockSize = 1;
        private TimeZone builderTimeZone = TimeZone.getDefault();
        private EncodingLevel builderEncodingLevel = EncodingLevel.EL0;
        private boolean builderPartitioned = false;
        private boolean builderNullsPadding = false;
        private Optional<List<Integer>> builderPartKeyColumnIds = Optional.empty();

        // added compared to PixelsWriterImpl
        private int builderPartitionId = -1;
        private URI builderUri = null;
        private String builderFileName = null;
        private List<String> builderFileNames = null;

        private Storage builderStorage = null;
        private String builderFilePath;

        private Builder()
        {
        }

        public Builder setStorage(Storage storage)
        {
            this.builderStorage = storage;
            return this;
        }

        public Builder setPath(String path)
        {
            this.builderFilePath = path;
            return this;
        }

        public Builder setSchema(TypeDescription schema)
        {
            this.builderSchema = requireNonNull(schema);
            return this;
        }

        public Builder setPixelStride(int stride)
        {
            this.builderPixelStride = stride;
            return this;
        }

        public Builder setRowGroupSize(int rowGroupSize)
        {
            this.builderRowGroupSize = rowGroupSize;
            return this;
        }

        public Builder setCompressionKind(PixelsProto.CompressionKind compressionKind)
        {
            this.builderCompressionKind = requireNonNull(compressionKind);
            return this;
        }

        public Builder setCompressionBlockSize(int compressionBlockSize)
        {
            this.builderCompressionBlockSize = compressionBlockSize;
            return this;
        }

        public Builder setTimeZone(TimeZone timeZone)
        {
            this.builderTimeZone = requireNonNull(timeZone);
            return this;
        }

        public Builder setNullsPadding(boolean nullsPadding)
        {
            this.builderNullsPadding = nullsPadding;
            return this;
        }

        public Builder setEncodingLevel(EncodingLevel encodingLevel)
        {
            this.builderEncodingLevel = encodingLevel;
            return this;
        }

        public Builder setPartitioned(boolean partitioned)
        {
            this.builderPartitioned = partitioned;
            return this;
        }

        public Builder setPartitionId(int partitionId)
        {
            this.builderPartitionId = partitionId;
            return this;
        }

        public Builder setPartKeyColumnIds(List<Integer> partitionColumnIds)
        {
            this.builderPartKeyColumnIds = Optional.ofNullable(partitionColumnIds);
            return this;
        }

        public Builder setFileName(String fileName)
        {
            this.builderFileName = requireNonNull(fileName);
            return this;
        }

        public Builder setFileNames(List<String> fileNames)
        {
            this.builderFileNames = requireNonNull(fileNames);
            return this;
        }

        public Builder setUri(URI uri)
        {
            this.builderUri = requireNonNull(uri);
            return this;
        }

        public PixelsWriter build() throws PixelsWriterException
        {
            requireNonNull(this.builderStorage, "storage is not set");
            requireNonNull(this.builderFilePath, "file path is not set");
            requireNonNull(this.builderSchema, "schema is not set");
            checkArgument(!requireNonNull(builderSchema.getChildren(), "schema's children is null").isEmpty(),
                    "schema is empty");
            checkArgument(this.builderPixelStride > 0, "pixels stride size is not set");
            checkArgument(this.builderRowGroupSize > 0, "row group size is not set");
            checkArgument(this.builderPartitioned ==
                            (this.builderPartKeyColumnIds.isPresent() && !this.builderPartKeyColumnIds.get().isEmpty()),
                    "partition column ids are present while partitioned is false, or vice versa");
            checkArgument(!this.builderPartitioned || this.builderPartitionId >= 0,
                    "partition id is not set while partitioned is true");
            checkArgument(!this.builderPartitioned || this.builderFileNames != null,
                    "file names are not set (partitioned: true)");
//            checkArgument(this.builderPartitioned || this.builderFileName != null || this.builderUri != null,
//                    "file name and uri not set (partitioned: false)");

            PhysicalWriter fsWriter = null;
            try
            {
                fsWriter = PhysicalWriterUtil.newPhysicalWriter(
                        this.builderStorage, this.builderFilePath, null);
            } catch (IOException e)
            {
                LOGGER.error("Failed to create PhysicalWriter");
                throw new PixelsWriterException(
                        "Failed to create PixelsWriter due to error of creating PhysicalWriter", e);
            }

            if (fsWriter == null)
            {
                LOGGER.error("Failed to create PhysicalWriter");
                throw new PixelsWriterException(
                        "Failed to create PixelsWriter due to error of creating PhysicalWriter");
            }

            return new PixelsWriterStreamImpl(
                    builderSchema,
                    builderPixelStride,
                    builderRowGroupSize,
                    builderCompressionKind,
                    builderCompressionBlockSize,
                    builderTimeZone,
                    fsWriter,
                    builderEncodingLevel,
                    builderNullsPadding,
                    builderPartitioned,
                    builderPartKeyColumnIds);
        }
    }

    private void writeHeader() throws IOException {
        requireNonNull(this.physicalWriter, "physical writer is not set");
        checkArgument(this.writtenBytes == 0, "written bytes is not 0");

        // build streamHeader
        PixelsStreamProto.StreamHeader.Builder streamHeaderBuilder = PixelsStreamProto.StreamHeader.newBuilder();
        writeTypes(streamHeaderBuilder, schema);
        streamHeaderBuilder.setVersion(PixelsVersion.currentVersion().getVersion())
                .setPixelStride(columnWriterOption.getPixelStride())
                .setWriterTimezone(timeZone.getDisplayName())
                .setPartitioned(partitioned)
                .setColumnChunkAlignment(CHUNK_ALIGNMENT)
                .setMagic(Constants.FILE_MAGIC)
                .build();
        PixelsStreamProto.StreamHeader streamHeader = streamHeaderBuilder.build();

        int streamHeaderLength = streamHeader.getSerializedSize();

        // write and flush streamHeader
//        this.byteBuf.writeInt(streamHeaderLength);
//        this.byteBuf.writeBytes(streamHeader.toByteArray());
        writtenBytes += streamHeaderLength + Integer.BYTES;

        // ensure the next member (row group data length) is aligned to CHUNK_ALIGNMENT
        int alignBytes = 0;
        if (CHUNK_ALIGNMENT != 0 && byteBuf.writerIndex() % CHUNK_ALIGNMENT != 0)
        {
            alignBytes = CHUNK_ALIGNMENT - byteBuf.writerIndex() % CHUNK_ALIGNMENT;
            writtenBytes += alignBytes;
        }

        checkArgument(this.writtenBytes >= Integer.MIN_VALUE && this.writtenBytes <= Integer.MAX_VALUE);
        ByteBuffer buf = ByteBuffer.allocate((int) this.writtenBytes);
        buf.putInt(streamHeaderLength);
        buf.put(streamHeader.toByteArray());
        buf.put(CHUNK_PADDING_BUFFER, 0, alignBytes);
        this.physicalWriter.append(buf);
        this.physicalWriter.flush();
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public TypeDescription getSchema()
    {
        return schema;
    }

    /**
     * Returns the num of already written row groups. (different from {@link PixelsWriterImpl#getNumRowGroup()})
     */
    @Override
    public int getNumRowGroup()
    {
        return rowGroupNum;
    }

    @Override
    public int getNumWriteRequests()
    {
        return rowGroupNum;
    }

    @Override
    public long getCompletedBytes()
    {
        return writtenBytes;
    }

    public int getPixelStride()
    {
        return columnWriterOption.getPixelStride();
    }

    public int getRowGroupSize()
    {
        return rowGroupSize;
    }

    public PixelsProto.CompressionKind getCompressionKind()
    {
        return compressionKind;
    }

    public int getCompressionBlockSize()
    {
        return compressionBlockSize;
    }

    public TimeZone getTimeZone()
    {
        return timeZone;
    }

    public EncodingLevel getEncodingLevel()
    {
        return columnWriterOption.getEncodingLevel();
    }

    public boolean isPartitioned()
    {
        return partitioned;
    }

//    @Override
//    public boolean addRowBatch(VectorizedRowBatch rowBatch) throws IOException
//    {
//        checkArgument(!partitioned,
//                "this file is hash partitioned, use addRowBatch(rowBatch, hashValue) instead");
//        /**
//         * Issue #170:
//         * ColumnWriter.write() returns the total size of the current column chunk,
//         * thus we should set curRowGroupDataLength = 0 here at the beginning.
//         */
//        curRowGroupDataLength = 0;
//        curRowGroupNumOfRows += rowBatch.size;
//        writeColumnVectors(rowBatch.cols, rowBatch.size);
//        // If the current row group size has exceeded the row group size, write current row group.
//        if (curRowGroupDataLength >= rowGroupSize)
//        {
//            writeRowGroup();
//            curRowGroupNumOfRows = 0L;
//            return false;
//        }
//        return true;
//    }

    @Override
    public boolean addRowBatch(VectorizedRowBatch rowBatch) throws IOException
    {
        checkArgument(!partitioned,
        "this file is hash partitioned, use addRowBatch(rowBatch, hashValue) instead");

        curRowGroupDataLength = 0;
        curRowGroupNumOfRows += rowBatch.size;
        writeColumnVectors(rowBatch.cols, rowBatch.size);
        // If the current row group size has exceeded the row group size, write current row group.
        if (curRowGroupDataLength >= rowGroupSize)
        {
            writeRowGroup();
            curRowGroupNumOfRows = 0L;
            return false;
        }
        return true;
    }

    @Override
    public void addRowBatch(VectorizedRowBatch rowBatch, int hashValue) throws IOException
    {
        checkArgument(partitioned,
                "this file is not hash partitioned, use addRowBatch(rowBatch) instead");
        if (hashValueIsSet)
        {
            // As the current hash value is set, at lease one row batch has been added.
            if (currHashValue != hashValue)
            {
                // Write the current hash partition (row group) and add the row batch to a new hash partition.
                writeRowGroup();
                curRowGroupNumOfRows = 0L;
            }
        }
        currHashValue = hashValue;
        hashValueIsSet = true;
        curRowGroupDataLength = 0;
        curRowGroupNumOfRows += rowBatch.size;
        writeColumnVectors(rowBatch.cols, rowBatch.size);
    }

    private void writeColumnVectors(ColumnVector[] columnVectors, int rowBatchSize)
    {
        CompletableFuture<?>[] futures = new CompletableFuture[columnVectors.length];
        AtomicInteger dataLength = new AtomicInteger(0);
        for (int i = 0; i < columnVectors.length; ++i)
        {
            CompletableFuture<Void> future = new CompletableFuture<>();
            ColumnWriter writer = columnWriters[i];
            ColumnVector columnVector = columnVectors[i];
            columnWriterService.execute(() -> {
                try
                {
                    dataLength.addAndGet(writer.write(columnVector, rowBatchSize));
                    future.complete(null);
                }
                catch (IOException e)
                {
                    throw new CompletionException("failed to write column vector", e);
                }
            });
            futures[i] = future;
        }
        CompletableFuture.allOf(futures).join();
        curRowGroupDataLength += dataLength.get();
    }

    /**
     * Close PixelsWriterStreamImpl, indicating the end of stream.
     */
    @Override
    public void close()
    {
        try
        {
            if (curRowGroupNumOfRows != 0)
            {
                writeRowGroup();
            }
            physicalWriter.close();
            for (ColumnWriter cw : columnWriters)
            {
                cw.close();
            }
            columnWriterService.shutdown();
            columnWriterService.shutdownNow();
        }
        catch (IOException e)
        {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }
    }
//    @Override
//    public void close()
//    {
//        try
//        {
//            if (curRowGroupNumOfRows != 0)
//            {
//                writeRowGroup();
//            }
//            // If the outgoing stream is empty (addRowBatch() and thus writeRowGroup() never called), we artificially
//            // send an empty row group here before closing,
//            //  so that the HTTP server can properly move on and close.
//            else if (isFirstRowGroup)
//            {
//                writeRowGroup();
//                isFirstRowGroup = false;
//            }
//
//            // In non-partitioned mode and for data servers, we send a close request with empty content to the server.
//            // In partitioned mode, the server closes automatically when it receives all its partitions. No need to send
//            //  a close request.
//            // Schema servers also close automatically and do not need close requests.
//            if (!partitioned && partitionId != PARTITION_ID_SCHEMA_WRITER)
//            {
//                if (!partitioned && uri == null)
//                {
//                    uri = URI.create(fileNameToUri(fileName));
//                }
//                Request req = httpClient
//                        .preparePost(partitioned ? uris.get(currHashValue).toString() : uri.toString())
//                        .addHeader(CONTENT_TYPE, "application/x-protobuf")
//                        .addHeader(CONTENT_LENGTH, 0)
//                        .addHeader(CONNECTION, CLOSE)
//                        .build();
//
//                outstandingHTTPRequestSemaphore.acquire();
//                Response response = httpClient.executeRequest(req).get();
//                if (response.getStatusCode() != 200)
//                {
//                    throw new IOException("Failed to send close request to server. Is the server already closed? " +
//                            "HTTP status code: " + response.getStatusCode());
//                }
//            }
//
//            for (ColumnWriter cw : columnWriters)
//            {
//                cw.close();
//            }
//            columnWriterService.shutdown();
//            columnWriterService.shutdownNow();
//
//            if (byteBuf.refCnt() > 0)
//            {
//                byteBuf.release();
//            }
//        }
//        catch (Exception e)
//        {
//            LOGGER.error("error when closing writer", e);
//        }
//    }

    private void writeRowGroup() throws IOException
    {
        PixelsProto.RowGroupIndex.Builder curRowGroupIndex = PixelsProto.RowGroupIndex.newBuilder();
        PixelsProto.RowGroupEncoding.Builder curRowGroupEncoding = PixelsProto.RowGroupEncoding.newBuilder();
        PixelsProto.PartitionInformation.Builder curPartitionInfo = PixelsProto.PartitionInformation.newBuilder();

        // reset each column writer and get current row group content size in bytes
        int rowGroupDataLength = 0;
        for (ColumnWriter writer : columnWriters)
        {
            // flush writes the isNull bit map into the internal output stream.
            writer.flush();
            rowGroupDataLength += writer.getColumnChunkSize();
            if (CHUNK_ALIGNMENT != 0 && rowGroupDataLength % CHUNK_ALIGNMENT != 0)
            {
                /*
                 * Issue #519:
                 * This is necessary as the prepare() method of some storage (e.g., hdfs)
                 * has to determine whether to start a new block, if the current block
                 * is not large enough.
                 */
                rowGroupDataLength += CHUNK_ALIGNMENT - rowGroupDataLength % CHUNK_ALIGNMENT;
            }
        }

        // write row group data length first
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.order(WRITER_ENDIAN).putInt(rowGroupDataLength);
        physicalWriter.append(buf);
        tmpFileBuf.put(buf);
        writtenBytes += 4;

        int tryAlign = 0;
//        while (CHUNK_ALIGNMENT != 0 && writtenBytes % CHUNK_ALIGNMENT != 0 && tryAlign++ < 2)
//        {
//            int alignBytes = (int) (CHUNK_ALIGNMENT - writtenBytes % CHUNK_ALIGNMENT);
//            physicalWriter.append(CHUNK_PADDING_BUFFER, 0, alignBytes);
//            writtenBytes += alignBytes;
//        }
//        if (tryAlign > 2)
//        {
//            throw new IOException("failed to align the start offset of the column chunks in the row group");
//        }

        // write and flush row group content
        int chunkOffset = 0;
        try
        {
            for (int i = 0; i < columnWriters.length; i++)
            {
                ColumnWriter writer = columnWriters[i];
                byte[] columnChunkBuf = writer.getColumnChunkContent();

                // DESIGN: Because ColumnChunkIndex does not change after the last write() or flush(),
                //  we have moved it from rowGroup footer (as in PixelsWriterImpl) to header here,
                //  which might work better with the streaming nature of this stream writer.
                PixelsProto.ColumnChunkIndex.Builder chunkIndexBuilder = writer.getColumnChunkIndex();
                chunkIndexBuilder.setChunkOffset(chunkOffset);
                chunkIndexBuilder.setChunkLength(columnChunkBuf.length);
                curRowGroupIndex.addColumnChunkIndexEntries(chunkIndexBuilder.build());
                curRowGroupEncoding.addColumnChunkEncodings(writer.getColumnChunkEncoding().build());

                physicalWriter.append(columnChunkBuf, 0, columnChunkBuf.length);
                tmpFileBuf.put(columnChunkBuf, 0, columnChunkBuf.length);
                writtenBytes += columnChunkBuf.length;
                chunkOffset += columnChunkBuf.length;
                // add align bytes to make sure the column size is the multiple of fsBlockSize
                if(CHUNK_ALIGNMENT != 0 && columnChunkBuf.length % CHUNK_ALIGNMENT != 0)
                {
                    int alignBytes = CHUNK_ALIGNMENT - columnChunkBuf.length % CHUNK_ALIGNMENT;
                    physicalWriter.append(CHUNK_PADDING_BUFFER, 0, alignBytes);
                    tmpFileBuf.put(CHUNK_PADDING_BUFFER, 0, alignBytes);
                    writtenBytes += alignBytes;
                    chunkOffset += alignBytes;
                }
                columnWriters[i] = newColumnWriter(children.get(i), columnWriterOption);
            }
            physicalWriter.flush();
        } catch (IOException e)
        {
            LOGGER.error(e.getMessage());
            throw e;
        }

        // write writer to output file
        try (FileOutputStream fos = new FileOutputStream("/tmp/test2");
             FileChannel fileChannel = fos.getChannel()) {
            // 将 ByteBuffer 写入文件
            fileChannel.write(tmpFileBuf);
            System.out.println("数据已写入文件 output.txt");
        } catch (Exception e) {
            e.printStackTrace();
        }
        tmpFileBuf.clear();

        if (partitioned)
        {
            // partitionColumnIds has been checked to be present in the builder.
            curPartitionInfo.addAllColumnIds(partKeyColumnIds.orElse(null));
            curPartitionInfo.setHashValue(currHashValue);
        }

        // put curRowGroupIndex into rowGroupFooter
        PixelsStreamProto.StreamRowGroupFooter.Builder rowGroupFooterBuilder =
                PixelsStreamProto.StreamRowGroupFooter.newBuilder()
                        .setRowGroupIndexEntry(curRowGroupIndex.build())
                        .setRowGroupEncoding(curRowGroupEncoding.build())
                        .setNumberOfRows(curRowGroupNumOfRows);
        if (partitioned)
        {
            rowGroupFooterBuilder.setPartitionInfo(curPartitionInfo.build());
        }
        PixelsStreamProto.StreamRowGroupFooter rowGroupFooter = rowGroupFooterBuilder.build();

        // write and flush row group footer
        byte[] footerBuffer = rowGroupFooter.toByteArray();
        buf.clear();
        buf.order(WRITER_ENDIAN).putInt(footerBuffer.length);
        physicalWriter.append(buf);
//        tmpFileBuf.put(buf.array());
        writtenBytes += 4;

//        tryAlign = 0;
//        while (CHUNK_ALIGNMENT != 0 && writtenBytes % CHUNK_ALIGNMENT != 0 && tryAlign++ < 2)
//        {
//            int alignBytes = (int) (CHUNK_ALIGNMENT - writtenBytes % CHUNK_ALIGNMENT);
//            physicalWriter.append(CHUNK_PADDING_BUFFER, 0, alignBytes);
//            writtenBytes += alignBytes;
//        }
//        if (tryAlign > 2)
//        {
//            throw new IOException("failed to align the start offset of the column chunks in the row group");
//        }
        physicalWriter.append(footerBuffer, 0, footerBuffer.length);
        physicalWriter.flush();
//        tmpFileBuf.put(footerBuffer, 0, footerBuffer.length);
        writtenBytes += footerBuffer.length;
        rowGroupNum++;
    }

    static void writeTypes(PixelsStreamProto.StreamHeader.Builder builder, TypeDescription schema)
    {
        List<TypeDescription> children = schema.getChildren();
        List<String> names = schema.getFieldNames();
        if (children == null || children.isEmpty())
        {
            return;
        }
        for (int i = 0; i < children.size(); i++)
        {
            TypeDescription child = children.get(i);
            PixelsProto.Type.Builder tmpType = PixelsProto.Type.newBuilder();
            tmpType.setName(names.get(i));
            switch (child.getCategory())
            {
                case BOOLEAN:
                    tmpType.setKind(PixelsProto.Type.Kind.BOOLEAN);
                    break;
                case BYTE:
                    tmpType.setKind(PixelsProto.Type.Kind.BYTE);
                    break;
                case SHORT:
                    tmpType.setKind(PixelsProto.Type.Kind.SHORT);
                    break;
                case INT:
                    tmpType.setKind(PixelsProto.Type.Kind.INT);
                    break;
                case LONG:
                    tmpType.setKind(PixelsProto.Type.Kind.LONG);
                    break;
                case FLOAT:
                    tmpType.setKind(PixelsProto.Type.Kind.FLOAT);
                    break;
                case DOUBLE:
                    tmpType.setKind(PixelsProto.Type.Kind.DOUBLE);
                    break;
                case DECIMAL:
                    tmpType.setKind(PixelsProto.Type.Kind.DECIMAL);
                    tmpType.setPrecision(child.getPrecision());
                    tmpType.setScale(child.getScale());
                    break;
                case STRING:
                    tmpType.setKind(PixelsProto.Type.Kind.STRING);
                    break;
                case CHAR:
                    tmpType.setKind(PixelsProto.Type.Kind.CHAR);
                    tmpType.setMaximumLength(child.getMaxLength());
                    break;
                case VARCHAR:
                    tmpType.setKind(PixelsProto.Type.Kind.VARCHAR);
                    tmpType.setMaximumLength(child.getMaxLength());
                    break;
                case BINARY:
                    tmpType.setKind(PixelsProto.Type.Kind.BINARY);
                    tmpType.setMaximumLength(child.getMaxLength());
                    break;
                case VARBINARY:
                    tmpType.setKind(PixelsProto.Type.Kind.VARBINARY);
                    tmpType.setMaximumLength(child.getMaxLength());
                    break;
                case TIMESTAMP:
                    tmpType.setKind(PixelsProto.Type.Kind.TIMESTAMP);
                    tmpType.setPrecision(child.getPrecision());
                    break;
                case DATE:
                    tmpType.setKind(PixelsProto.Type.Kind.DATE);
                    break;
                case TIME:
                    tmpType.setKind(PixelsProto.Type.Kind.TIME);
                    tmpType.setPrecision(child.getPrecision());
                    break;
                default:
                    throw new IllegalArgumentException("Unknown category: " + schema.getCategory());
            }
            builder.addTypes(tmpType.build());
        }
    }

    private void writeStreamHeader()
    {
        // build streamHeader
        PixelsStreamProto.StreamHeader.Builder streamHeaderBuilder = PixelsStreamProto.StreamHeader.newBuilder();
        writeTypes(streamHeaderBuilder, schema);
        streamHeaderBuilder.setVersion(PixelsVersion.currentVersion().getVersion())
                .setPixelStride(columnWriterOption.getPixelStride())
                .setWriterTimezone(timeZone.getDisplayName())
                .setPartitioned(partitioned)
                .setColumnChunkAlignment(CHUNK_ALIGNMENT)
                .setMagic(Constants.FILE_MAGIC)
                .build();
        PixelsStreamProto.StreamHeader streamHeader = streamHeaderBuilder.build();
        int streamHeaderLength = streamHeader.getSerializedSize();

        // write and flush streamHeader
        byte[] magicBytes = FILE_MAGIC.getBytes();
        byteBuf.writeBytes(magicBytes);
        byteBuf.writeInt(streamHeaderLength);
        byteBuf.writeBytes(streamHeader.toByteArray());
        writtenBytes += magicBytes.length + streamHeaderLength + Integer.BYTES;

        int paddingLength = (8 - (magicBytes.length + Integer.BYTES + streamHeaderLength) % 8) % 8;  // Can use '&7'
        byte[] paddingBytes = new byte[paddingLength];
        byteBuf.writeBytes(paddingBytes);
        writtenBytes += paddingLength;

        // ensure the next member (row group data length) is aligned to CHUNK_ALIGNMENT
        if (CHUNK_ALIGNMENT != 0 && byteBuf.writerIndex() % CHUNK_ALIGNMENT != 0)
        {
            int alignBytes = CHUNK_ALIGNMENT - byteBuf.writerIndex() % CHUNK_ALIGNMENT;
            byteBuf.writeBytes(CHUNK_PADDING_BUFFER, 0, alignBytes);
            writtenBytes += alignBytes;
        }
    }
}
