package org.locationtech.geogig.web.api;

import java.io.IOException;
import java.io.OutputStream;

import org.locationtech.geogig.rest.ByteRepresentation;
import org.restlet.data.MediaType;

public class ByteWriterRepresentation extends ByteRepresentation {
	final ByteResponse impl;

    public ByteWriterRepresentation(MediaType mediaType, ByteResponse impl) {
        super(mediaType);
        this.impl = impl;
    }

    @Override
    public void write(OutputStream writer) throws IOException {
        try {
            impl.write(writer);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
