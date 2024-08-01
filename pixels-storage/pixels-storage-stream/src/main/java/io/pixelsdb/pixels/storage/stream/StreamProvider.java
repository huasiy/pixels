package io.pixelsdb.pixels.storage.stream;

import io.pixelsdb.pixels.common.physical.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

public class StreamProvider implements StorageProvider
{
    @Override
    public Storage createStorage(@Nonnull Storage.Scheme scheme) throws IOException
    {
        if (!this.compatibleWith(scheme))
        {
            throw new IOException("incompatible storage scheme: " + scheme);
        }
        return new Stream();
    }

    @Override
    public PhysicalReader createReader(@Nonnull Storage storage, @Nonnull String path,
                                       @Nullable PhysicalReaderOption option) throws IOException
    {
        if (!this.compatibleWith(storage.getScheme()))
        {
            throw new IOException("incompatible storage scheme: " + storage.getScheme());
        }
        return new PhysicalStreamReader(storage, path);
    }

    @Override
    public PhysicalWriter createWriter(@Nonnull Storage storage, @Nonnull String path,
                                       @Nonnull PhysicalWriterOption option) throws IOException
    {
        if (!this.compatibleWith(storage.getScheme()))
        {
            throw new IOException("incompatible storage scheme: " + storage.getScheme());
        }
        return new PhysicalStreamWriter(storage, path);
    }

    @Override
    public boolean compatibleWith(@Nonnull Storage.Scheme scheme)
    {
        return scheme.equals(Storage.Scheme.stream);
    }
}
