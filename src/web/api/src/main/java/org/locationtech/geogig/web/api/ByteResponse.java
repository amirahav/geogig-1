/* Copyright (c) 2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Johnathan Garrett (LMN Solutions) - initial implementation
 */
package org.locationtech.geogig.web.api;

import java.io.OutputStream;
import java.io.Writer;

/**
 * Provides a base abstract streaming response implementation for Web API commands.
 */
public abstract class ByteResponse {

    /**
     * Write the command response to the provided {@link Writer}.
     * 
     * @param out the output stream
     * @throws Exception
     */
    public abstract void write(OutputStream out) throws Exception;

}