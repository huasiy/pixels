package io.pixelsdb.pixels.storage.stream;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class PhysicalStreamReader implements PhysicalReader
{
    private final Stream stream;
    private final String path;

    public PhysicalStreamReader(Storage storage, String path) throws IOException
    {
        if (storage instanceof Stream)
        {
            this.stream = (Stream) storage;
        }
        else
        {
            throw new IOException("Storage is not HDFS.");
        }
    }

    @Override
    public long getFileLength() throws IOException {
        return 0;
    }

    @Override
    public void seek(long desired) throws IOException {

    }

    @Override
    public ByteBuffer readFully(int length) throws IOException {
        return null;
    }

    @Override
    public void readFully(byte[] buffer) throws IOException {

    }

    @Override
    public void readFully(byte[] buffer, int offset, int length) throws IOException {

    }

    @Override
    public long readLong(ByteOrder byteOrder) throws IOException {
        return 0;
    }

    @Override
    public int readInt(ByteOrder byteOrder) throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public String getPath() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public long getBlockId() throws IOException {
        return 0;
    }

    @Override
    public Storage.Scheme getStorageScheme() {
        return null;
    }

    @Override
    public int getNumReadRequests() {
        return 0;
    }
}
