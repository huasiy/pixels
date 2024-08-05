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

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.join.Partitioner;
import io.pixelsdb.pixels.executor.predicate.ColumnFilter;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.executor.scan.Scanner;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.worker.common.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author jasha64
 * @create 2023-08-07
 */
public class TestHttpServerClient
{

    @Test
    public void testServerSimple() throws Exception
    {
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.enableEncodedColumnVector(false);
        String[] colNames = new String[]{"n_nationkey", "n_name", "n_regionkey", "n_comment"};
        option.includeCols(colNames);

        String serverIpAddress = "127.0.0.1";
        int serverPort = 50100;
        PixelsReaderStreamImpl reader = new PixelsReaderStreamImpl(
                "http://" + serverIpAddress + ":" + serverPort + "/");
        PixelsRecordReader recordReader = reader.read(option);
        // use an array of readers, to support multiple streams (relies on
        //  a service framework to map endpoints to IDs. todo)
        while (true)
        {
            try
            {
                VectorizedRowBatch rowBatch = recordReader.readBatch(5);
                if (rowBatch.size == 0)
                {
                    reader.close();
                    break;
                }
                System.out.println("Parsed rowBatch: ");
                System.out.println(rowBatch);
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    void runAsync(Runnable fp, int concurrency)
    {
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        CompletableFuture<Void>[] httpServerFutures = new CompletableFuture[concurrency];
        for (int i = 0; i < concurrency; i++)
        {
            httpServerFutures[i] = CompletableFuture.runAsync(() -> {
                try
                {
                    fp.run();
                } catch (Exception e)
                {
                    e.printStackTrace();
                }
            }, executorService);
            if (i < concurrency - 1)
            {
                System.out.println("Booted " + (i + 1) + " http clients");
                try
                {
                    Thread.sleep(1000);
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        }

        try
        {
            for (int i = 10; i > 0; i--)
            {
                System.out.printf("Main thread is still running... %d\n", i);
                Thread.sleep(1000);
            }
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(httpServerFutures);
        combinedFuture.join();

        executorService.shutdown();
    }

    @Test
    public void testServerAsync()
    {
        runAsync(() -> {
            try
            {
                testServerSimple();
            } catch (Exception e)
            {
                e.printStackTrace();
            }
        }, 1);
    }

    @Test
    public void testClientSimple() throws IOException
    {
        Storage fileStorage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        PixelsReaderImpl.Builder reader = PixelsReaderImpl.newBuilder()
                .setStorage(fileStorage)
                .setPath("/home/jasha/pixels-tpch/nation/v-0-ordered/20230814143629_105.pxl")
                .setEnableCache(false)
                .setPixelsFooterCache(new PixelsFooterCache());
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.enableEncodedColumnVector(false);
        String[] colNames = new String[]{"n_nationkey", "n_name", "n_regionkey", "n_comment"};
        option.includeCols(colNames);
        PixelsRecordReader recordReader = reader.build().read(option);

        String serverIpAddress = "127.0.0.1";
        int serverPort = 50100;
        PixelsWriter pixelsWriter = PixelsWriterStreamImpl.newBuilder()
                .setUri(URI.create("http://" + serverIpAddress + ":" + serverPort + "/"))
                .setSchema(recordReader.getResultSchema())
                .setPixelStride(10000)
                .setRowGroupSize(1048576)  // send a packet per 1MB (segmentation possible)
                // .setOverwrite(true) // set overwrite to true to avoid existence checking.
                .setEncodingLevel(EncodingLevel.EL2)
                .setPartitioned(false)
                .build();
        // XXX: now we can send multiple rowBatches in one rowGroup in one packet, but have not tested to send
        //  multiple rowGroups
        while (true)
        {
            VectorizedRowBatch rowBatch = recordReader.readBatch(5);
            System.out.println(rowBatch.size + " rows read from tpch nation.pxl");

            try
            {
                if (rowBatch.size == 0)
                {
                    pixelsWriter.close();
                    break;
                } else pixelsWriter.addRowBatch(rowBatch);
            } catch (Throwable e)
            {
                throw new WorkerException("failed to write rowBatch to HTTP server", e);
            }
        }
    }

    @Test
    public void testClientAsync()
    {
        runAsync(() -> {
            try
            {
                testClientSimple();
            } catch (Exception e)
            {
                e.printStackTrace();
            }
        }, 1);
    }

    @Test
    public void testClientConcurrent()
    {
        runAsync(() -> {
            try
            {
                testClientSimple();
            } catch (Exception e)
            {
                e.printStackTrace();
            }
        }, 3);
    }

    public void runLeftPartitioner()
    {
        PartitionOutput partitionOutput = new PartitionOutput();
        int numPartition = 8;
        long transId = 123456;
        String filePath = "/home/hsy/Downloads/nation.pxl";
        StorageInfo inputStorage = new StorageInfo(Storage.Scheme.file, "", "", "", "");
        StorageInfo outputStorage = new StorageInfo(Storage.Scheme.stream, "", "", "", "");
        WorkerCommon.initStorage(inputStorage);
        WorkerCommon.initStorage(outputStorage);

        InputInfo scanInput = new InputInfo(filePath, 0 ,32);
        List<InputInfo> scanInputs = new ArrayList<>();
        scanInputs.add(scanInput);
        InputSplit inputSplit = new InputSplit();
        inputSplit.setInputInfos(scanInputs);
        List<InputSplit> inputSplits = new ArrayList<>();
        inputSplits.add(inputSplit);

        String[] colNames = new String[]{"n_nationkey", "n_name", "n_regionkey", "n_comment"};
        int[] keyColumnIds = new int[]{0};
        boolean[] projections = new boolean[]{true, true, true, true};
        boolean encoding = true;
        SortedMap<Integer, ColumnFilter> columnFilters = new TreeMap<>();
        TableScanFilter filter = new TableScanFilter("tpch", "nation", columnFilters);
        ExecutorService threadPool = Executors.newFixedThreadPool(1);
        List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitioned = new ArrayList<>(numPartition);
        AtomicReference<TypeDescription> writerSchema = new AtomicReference<>();
        for (int i = 0; i < numPartition; ++i)
        {
            partitioned.add(new ConcurrentLinkedQueue<>());
        }
        for (InputSplit inputSplit1 : inputSplits)
        {
            List<InputInfo> scanInputs1 = inputSplit1.getInputInfos();
            threadPool.execute(() -> {
                try
                {
                    partitionFile(transId, scanInputs1, colNames, inputStorage.getScheme(), filter, keyColumnIds,
                            projections, partitioned, writerSchema);
                } catch (Throwable e)
                {
                    throw new WorkerException("error during partitioning", e);
                }
            });
        }
        threadPool.shutdown();
        try
        {
            while (!threadPool.awaitTermination(60, TimeUnit.SECONDS));
        } catch (InterruptedException e)
        {
            throw new WorkerException("interrupted while waiting for the termination of partitioning", e);
        }

        try
        {
            if (writerSchema.get() == null)
            {
                TypeDescription fileSchema = WorkerCommon.getFileSchemaFromSplits(
                        WorkerCommon.getStorage(inputStorage.getScheme()), inputSplits);
                TypeDescription resultSchema = WorkerCommon.getResultSchema(fileSchema, colNames);
                writerSchema.set(resultSchema);
            }
            String[] downStreamWorkers = new String[] {"127.0.0.1:50010", "127.0.0.1:50011", "127.0.0.1:50012",
                    "127.0.0.1:50013", "127.0.0.1:50014", "127.0.0.1:50015", "127.0.0.1:50016", "127.0.0.1:50017",};
            Set<Integer> hashValues = new HashSet<>(numPartition);
            for (int hash = 0; hash < numPartition; ++hash)
            {
                ConcurrentLinkedQueue<VectorizedRowBatch> batches = partitioned.get(hash);
                String worker = downStreamWorkers[hash];
                PixelsWriter pixelsWriter = WorkerCommon.getWriter(writerSchema.get(),
                        WorkerCommon.getStorage(outputStorage.getScheme()), worker, encoding,
                        false, Arrays.stream(keyColumnIds).boxed().collect(Collectors.toList()));
                if (!batches.isEmpty())
                {
                    for (VectorizedRowBatch batch : batches)
                    {
                        pixelsWriter.addRowBatch(batch, hash);
                    }
                    hashValues.add(hash);
                }
                pixelsWriter.close();
            }
            partitionOutput.setHashValues(hashValues);
        } catch (Exception e)
        {
            System.out.println("error during partitioning: " + e);
            partitionOutput.setSuccessful(false);
        }
    }

    public void runRightPartitioner()
    {
        PartitionOutput partitionOutput = new PartitionOutput();
        int numPartition = 8;
        long transId = 123456;
        String filePath = "/home/hsy/Downloads/supplier.pxl";
        StorageInfo inputStorage = new StorageInfo(Storage.Scheme.file, "", "", "", "");
        StorageInfo outputStorage = new StorageInfo(Storage.Scheme.stream, "", "", "", "");
        WorkerCommon.initStorage(inputStorage);
        WorkerCommon.initStorage(outputStorage);

        InputInfo scanInput = new InputInfo(filePath, 0 ,16);
        List<InputInfo> scanInputs = new ArrayList<>();
        scanInputs.add(scanInput);
        InputSplit inputSplit = new InputSplit();
        inputSplit.setInputInfos(scanInputs);
        List<InputSplit> inputSplits = new ArrayList<>();
        inputSplits.add(inputSplit);

        String[] colNames = new String[]{"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"};
        int[] keyColumnIds = new int[]{3};
        boolean[] projections = new boolean[]{true, true, true, true, true, true, true};
        boolean encoding = true;
        SortedMap<Integer, ColumnFilter> columnFilters = new TreeMap<>();
        TableScanFilter filter = new TableScanFilter("tpch", "supplier", columnFilters);
        ExecutorService threadPool = Executors.newFixedThreadPool(1);
        List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitioned = new ArrayList<>(numPartition);
        AtomicReference<TypeDescription> writerSchema = new AtomicReference<>();
        for (int i = 0; i < numPartition; ++i)
        {
            partitioned.add(new ConcurrentLinkedQueue<>());
        }
        for (InputSplit inputSplit1 : inputSplits)
        {
            List<InputInfo> scanInputs1 = inputSplit1.getInputInfos();
            threadPool.execute(() -> {
                try
                {
                    partitionFile(transId, scanInputs1, colNames, inputStorage.getScheme(), filter, keyColumnIds,
                            projections, partitioned, writerSchema);
                } catch (Throwable e)
                {
                    throw new WorkerException("error during partitioning", e);
                }
            });
        }
        threadPool.shutdown();
        try
        {
            while (!threadPool.awaitTermination(60, TimeUnit.SECONDS));
        } catch (InterruptedException e)
        {
            throw new WorkerException("interrupted while waiting for the termination of partitioning", e);
        }

        try
        {
            if (writerSchema.get() == null)
            {
                TypeDescription fileSchema = WorkerCommon.getFileSchemaFromSplits(
                        WorkerCommon.getStorage(inputStorage.getScheme()), inputSplits);
                TypeDescription resultSchema = WorkerCommon.getResultSchema(fileSchema, colNames);
                writerSchema.set(resultSchema);
            }
            String[] downStreamWorkers = new String[] {"127.0.0.1:50018", "127.0.0.1:50019", "127.0.0.1:50020",
            "127.0.0.1:50021", "127.0.0.1:50022", "127.0.0.1:50023", "127.0.0.1:50024", "127.0.0.1:50025"};
            Set<Integer> hashValues = new HashSet<>(numPartition);
            for (int hash = 0; hash < numPartition; ++hash)
            {
                ConcurrentLinkedQueue<VectorizedRowBatch> batches = partitioned.get(hash);
                String worker = downStreamWorkers[hash];
                PixelsWriter pixelsWriter = WorkerCommon.getWriter(writerSchema.get(),
                        WorkerCommon.getStorage(outputStorage.getScheme()), worker, encoding,
                        false, Arrays.stream(keyColumnIds).boxed().collect(Collectors.toList()));
                if (!batches.isEmpty())
                {
                    for (VectorizedRowBatch batch : batches)
                    {
                        pixelsWriter.addRowBatch(batch, hash);
                    }
                    hashValues.add(hash);
                }
                pixelsWriter.close();
            }
            partitionOutput.setHashValues(hashValues);
        } catch (Exception e)
        {
            System.out.println("error during partitioning: " + e);
            partitionOutput.setSuccessful(false);
        }
    }



    public void runPartitionedJoiner(int partitionId)
    {
        JoinOutput joinOutput = new JoinOutput();
        try
        {
            ExecutorService threadPool = Executors.newFixedThreadPool(1);
            int numPartition = 8;
            long transId = 123456;


        } catch (Exception e)
        {
            throw new WorkerException("error running partitionedJoiner ", e);
        }
    }

    @Test
    public void testJoin()
    {
        int numPartitions = 8;
        int numLeftPartitioner = 4;
        int numRightPartitioner = 4;
        // 左表和右表分别启动4个partitioner
        runAsync(() -> {
            try
            {
                runLeftPartitioner();
            } catch (Exception e)
            {
                e.printStackTrace();
            }
        }, numLeftPartitioner);
        runAsync(() -> {
            try
            {
                runRightPartitioner();
            } catch (Exception e)
            {
                e.printStackTrace();
            }
        }, numRightPartitioner);

        ExecutorService executor = Executors.newFixedThreadPool(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            int partitionId = i;
            executor.submit(() -> {
               try
               {
                   runPartitionedJoiner(partitionId);
               } catch (Exception e)
               {
                   e.printStackTrace();
               }
            });
        }
        StorageInfo leftStorage = new StorageInfo(Storage.Scheme.stream, "", "", "", "");
        StorageInfo rightStorage = new StorageInfo(Storage.Scheme.stream, "", "", "", "");
        StorageInfo outputStorage = new StorageInfo(Storage.Scheme.file, "", "", "", "");
        WorkerCommon.initStorage(leftStorage);
        WorkerCommon.initStorage(rightStorage);
        WorkerCommon.initStorage(outputStorage);


    }

    void partitionFile(long transId, List<InputInfo> scanInputs,
                       String[] columnsToRead, Storage.Scheme inputScheme,
                       TableScanFilter filter, int[] keyColumnIds, boolean[] projection,
                       List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitionResult,
                       AtomicReference<TypeDescription> writerSchema)
    {
        Scanner scanner = null;
        Partitioner partitioner = null;
        for (InputInfo inputInfo : scanInputs)
        {
            try (PixelsReader pixelsReader = WorkerCommon.getReader(
                    inputInfo.getPath(), WorkerCommon.getStorage(inputScheme)))
            {
                // TODO(hsy): support get row group num
                // ...
                PixelsReaderOption option = WorkerCommon.getReaderOption(transId, columnsToRead, inputInfo);
                PixelsRecordReader recordReader = pixelsReader.read(option);
                TypeDescription rowBatchSchema = recordReader.getResultSchema();
                VectorizedRowBatch rowBatch;

                if (scanner == null)
                {
                    scanner = new Scanner(WorkerCommon.rowBatchSize, rowBatchSchema, columnsToRead, projection, filter);
                }
                if (partitioner == null)
                {
                    partitioner = new Partitioner(partitionResult.size(), WorkerCommon.rowBatchSize,
                            scanner.getOutputSchema(), keyColumnIds);
                }
                if (writerSchema.get() == null)
                {
                    writerSchema.weakCompareAndSet(null, scanner.getOutputSchema());
                }

                do
                {
                    rowBatch = scanner.filterAndProject(recordReader.readBatch(WorkerCommon.rowBatchSize));
                    if (rowBatch.size > 0)
                    {
                        Map<Integer, VectorizedRowBatch> result = partitioner.partition(rowBatch);
                        if (!result.isEmpty())
                        {
                            for (Map.Entry<Integer, VectorizedRowBatch> entry : result.entrySet())
                            {
                                partitionResult.get(entry.getKey()).add(entry.getValue());
                            }
                        }
                    }
                } while (rowBatch.size > 0);
            } catch (Throwable e)
            {
                throw new WorkerException("failed to scan the file '" +
                        inputInfo.getPath() + "' and output the partitioning result", e);
            }
        }
        if (partitioner != null)
        {
            VectorizedRowBatch[] tailBatches = partitioner.getRowBatches();
            for (int hash = 0; hash < tailBatches.length; ++hash)
            {
                if (!tailBatches[hash].isEmpty())
                {
                    partitionResult.get(hash).add(tailBatches[hash]);
                }
            }
        }
    }
}
