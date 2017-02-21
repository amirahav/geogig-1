/* Copyright (c) 2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Erik Merkle (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.datastream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.storage.impl.ObjectSerializingFactory;

import com.google.common.base.Preconditions;
import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;

/**
 * Wrapper Factory that deflates/inflates data written to/read from streams using LZF compression.
 */
public class LZFSerializationFactory implements ObjectSerializingFactory {

    private final ObjectSerializingFactory factory;

    public LZFSerializationFactory(final ObjectSerializingFactory factory) {
        Preconditions.checkNotNull(factory);
        this.factory = factory;
    }

    @Override
    public RevObject read(ObjectId id, InputStream rawData) throws IOException {
        // decompress the stream
        try (LZFInputStream inflatedInputeStream = new LZFInputStream(rawData)) {
            return factory.read(id, inflatedInputeStream);
        }
    }

    @Override
    public RevObject read(@Nullable ObjectId id, byte[] data, int offset, int length)
            throws IOException {
        byte[] decoded = LZFDecoder.decode(data, offset, length);
        return factory.read(id, decoded, 0, decoded.length);
    }

    @Override
    public void write(RevObject o, OutputStream out) throws IOException {
        // compress the stream
        try (LZFOutputStream deflatedOutputStream = new LZFOutputStream(out)) {
            factory.write(o, deflatedOutputStream);
        }
    }

    @Override
    public String getDisplayName() {
        return factory.getDisplayName() + "/LZF";
    }
}
