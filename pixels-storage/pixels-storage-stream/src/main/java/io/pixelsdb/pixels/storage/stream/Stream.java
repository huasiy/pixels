package io.pixelsdb.pixels.storage.stream;

import io.pixelsdb.pixels.common.physical.Location;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Stream implements Storage
{
    private static final Logger logger = LogManager.getLogger(Stream.class);
    private static final String SchemePrefix = "http://";

    public Stream()
    {}

    @Override
    public Scheme getScheme() { return Scheme.stream; }

    @Override
    public String ensureSchemePrefix(String path) throws IOException
    {
        if (path.startsWith(SchemePrefix))
        {
            return path;
        }
        if (!path.startsWith(SchemePrefix))
        {
            if (path.contains("://"))
            {
            throw new IOException("Path '" + path +
                    "' already has a different scheme prefix than '" + SchemePrefix + "'.");
        }

        return SchemePrefix + path;
    }

    @Override
    public List<Status> listStatus(String... path) throws IOException
    {
        // test http endpoint is ok
        List<Status> statuses = new ArrayList<>();
        for (String eachPath : path)
        {
            statuses.add(new Status());
        }
        return statuses;
    }

    @Override
    public List<String> listPaths(String... path) throws IOException
    {
        throw new IOException("no implement");
    }

    @Override
    public Status getStatus(String path) throws IOException
    {
        throw new IOException("no implement");
    }

    @Override
    public long getFileId(String path) throws IOException
    {
        throw new IOException("no implement");
    }

    @Override
    public List<Location> getLocations(String path) throws IOException
    {
        throw new IOException("no implement");
    }

    @Override
    public String[] getHosts(String path) throws IOException
    {
        throw new IOException("no implement");
    }

    @Override
    public boolean mkdirs(String path) throws IOException {
        throw new IOException("no implement");
    }

    @Override
    public DataInputStream open(String path) throws IOException {
        throw new IOException("no implement");
    }

    @Override
    public DataOutputStream create(String path, boolean overwrite, int bufferSize) throws IOException {
        throw new IOException("no implement");
    }

    @Override
    public boolean delete(String path, boolean recursive) throws IOException {
        throw new IOException("no implement");
    }

    @Override
    public boolean supportDirectCopy() {
        return false;
    }

    @Override
    public boolean directCopy(String src, String dest) throws IOException {
        throw new IOException("no implement");
    }

    @Override
    public void close() throws IOException {
        throw new IOException("no implement");
    }

    @Override
    public boolean exists(String path) throws IOException {
        throw new IOException("no implement");
    }

    @Override
    public boolean isFile(String path) throws IOException {
        throw new IOException("no implement");
    }

    @Override
    public boolean isDirectory(String path) throws IOException {
        throw new IOException("no implement");
    }


}
