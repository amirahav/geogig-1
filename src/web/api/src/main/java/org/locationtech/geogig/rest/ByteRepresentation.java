package org.locationtech.geogig.rest;

import java.io.IOException;
import java.io.OutputStream;

import org.restlet.data.MediaType;
import org.restlet.resource.OutputRepresentation;

public abstract class ByteRepresentation extends OutputRepresentation {

    public ByteRepresentation(MediaType mediaType) {
        super(mediaType);
    }

    @Override
    public void write(OutputStream outputStream) throws IOException {
        write(outputStream);
        outputStream.flush();
    }


}