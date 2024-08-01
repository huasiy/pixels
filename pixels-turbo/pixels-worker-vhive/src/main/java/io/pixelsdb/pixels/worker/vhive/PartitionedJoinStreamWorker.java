package io.pixelsdb.pixels.worker.vhive;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import io.pixelsdb.pixels.worker.common.WorkerThreadExceptionHandler;
import io.pixelsdb.pixels.worker.common.WorkerThreadFactory;
import io.pixelsdb.pixels.worker.vhive.utils.RequestHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.naming.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Objects.requireNonNull;

public class PartitionedJoinStreamWorker implements RequestHandler<PartitionedJoinInput, JoinOutput>
{
    private static final Logger logger = LogManager.getLogger(PartitionedJoinStreamWorker.class);
    private final WorkerContext context;
    public PartitionedJoinStreamWorker(WorkerContext context)
    {
        this.context = context;
    }
    @Override
    public JoinOutput handleRequest(PartitionedJoinInput input)
    {
        JoinOutput joinOutput = new JoinOutput();
        long startTime = System.currentTimeMillis();
        joinOutput.setStartTimeMs(startTime);
        joinOutput.setRequestId(context.getRequestId());
        joinOutput.setSuccessful(true);
        joinOutput.setErrorMessage("");

        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            WorkerThreadExceptionHandler exceptionHandler = new WorkerThreadExceptionHandler(logger);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2,
                    new WorkerThreadFactory(exceptionHandler));

            requireNonNull(input.getSmallTable(), "input.smallTable is null");
            requireNonNull(input.getLargeTable(), "input.largeTable is null");
            requireNonNull(input.getOutput(), "input.output is null");
            int leftParallelism = input.getSmallTable().getParallelism();
            int rightParallelism = input.getLargeTable().getParallelism();
            StorageInfo leftStorageInfo = input.getSmallTable().getStorageInfo();
            StorageInfo rightStorageInfo = input.getLargeTable().getStorageInfo();
            StorageInfo outputStorageInfo = input.getOutput().getStorageInfo();
            String[] leftColumnsToRead = input.getSmallTable().getColumnsToRead();
            int[] leftKeyColumnIds = input.getSmallTable().getKeyColumnIds();
            String[] rightColumnsToRead = input.getLargeTable().getColumnsToRead();
            int[] rightKeyColumnIds = input.getLargeTable().getKeyColumnIds();
            String[] leftColAlias = input.getJoinInfo().getSmallColumnAlias();
            String[] rightColAlias = input.getJoinInfo().getLargeColumnAlias();
            boolean[] leftProjection = input.getJoinInfo().getSmallProjection();
            boolean[] rightProjection = input.getJoinInfo().getLargeProjection();
            JoinType joinType = input.getJoinInfo().getJoinType();
            List<Integer> hashValues = input.getJoinInfo().getHashValues();
            int numPartition = input.getJoinInfo().getNumPartition();


            // left and right table must be streamed
            String endpoint = "localhost:";
            ArrayList<String> endpoints = new ArrayList<>();
            for (int i = 8890; i < 8890+numPartition; i++) {
                endpoints.add(endpoint + i);
            }
            StorageInfo leftStorageInfo = new StorageInfo(Storage.Scheme.stream_in, "", endpoints, "", "");
            endpoints.clear();
            for (int i = 8990; i < 8990+numPartition; i++) {
                endpoints.add(endpoint + i);
            }
            StorageInfo rightStorageInfo = new StorageInfo(Storage.Scheme.stream_in, "", endpoints, "", "");


        } catch (Throwable e)
        {
            logger.error("error during join", e);
            joinOutput.setSuccessful(false);
            joinOutput.setErrorMessage(e.getMessage());
            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
        }
        return joinOutput;
    }
    @Override
    public String getRequestId() { return context.getRequestId(); }
    @Override
    public WorkerType getWorkerType() { return WorkerType.PARTITIONED_JOIN_STREAM; }
}
