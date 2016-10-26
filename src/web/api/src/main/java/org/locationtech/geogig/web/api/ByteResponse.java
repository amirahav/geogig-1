package org.locationtech.geogig.web.api;

import java.io.OutputStream;

public abstract class ByteResponse {
    /**
     * Write the command response to the provided {@link Writer}.
     * 
     * @param out the output stream
     * @throws Exception
     */
    public abstract void write(OutputStream out) throws Exception;

}
