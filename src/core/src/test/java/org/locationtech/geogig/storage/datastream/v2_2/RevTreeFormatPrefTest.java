/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.datastream.v2_2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.locationtech.geogig.model.impl.RevObjectTestSupport.hashString;
import static org.locationtech.geogig.storage.datastream.v2_2.TestSupport.assertEqualsFully;
import static org.locationtech.geogig.storage.datastream.v2_2.TestSupport.nodes;
import static org.locationtech.geogig.storage.datastream.v2_2.TestSupport.tree;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.locationtech.geogig.model.Bucket;
import org.locationtech.geogig.model.Node;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevObject.TYPE;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV2;
import org.locationtech.geogig.storage.memory.HeapObjectStore;
import org.locationtech.geogig.test.performance.EnablePerformanceTestRule;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.ning.compress.lzf.LZFOutputStream;

/**
 * Check how fast can v2.2 encode and decode trees compared to v2.1
 */
public class RevTreeFormatPrefTest {

    @Rule
    public EnablePerformanceTestRule testEnabler = new EnablePerformanceTestRule();

    @Rule
    public TestName testName = new TestName();

    private ObjectStore store;

    @Before
    public void before() {
        store = new HeapObjectStore();
        store.open();
        System.err.printf("####### %s #######\n", testName.getMethodName());
    }

    @After
    public void after() {
        store.close();
    }

    @Test
    public void testEmptyTree() throws IOException {
        RevTree orig = RevTree.EMPTY;
        RevTree decoded = encodeDecode(orig);
        assertSame(orig, decoded);
    }

    @Test
    public void testFeatureLeafWithNonRepeatedMetadataIds() throws IOException {
        RevTree tree;
        List<Node> tNodes = nodes(TYPE.TREE, 1024, true, true, false);
        List<Node> fNodes = nodes(TYPE.FEATURE, 1024, true, true, false);
        tree = tree(2048, tNodes, fNodes, null);
        encodeDecode(tree);
    }

    @Test
    public void testFeatureLeafWithNonRepeatedMetadataIdsAndExtraData() throws IOException {
        RevTree tree;
        List<Node> tNodes = nodes(TYPE.TREE, 1024, true, true, true);
        List<Node> fNodes = nodes(TYPE.FEATURE, 1024, true, true, true);
        tree = tree(2048, tNodes, fNodes, null);
        encodeDecode(tree);
    }

    @Test
    public void testFeatureLeafWithRepeatedMetadataIds() throws IOException {
        RevTree tree;
        List<ObjectId> repeatingMdIds = ImmutableList.of(//
                hashString("mdid1"), //
                hashString("mdid2"), //
                hashString("mdid3"));
        List<Node> tNodes = nodes(TYPE.TREE, 1024, repeatingMdIds, true, false);
        List<Node> fNodes = nodes(TYPE.FEATURE, 1024, repeatingMdIds, true, false);
        tree = tree(2048, tNodes, fNodes, null);
        encodeDecode(tree);
    }

    @Test
    public void testFeatureLeafWithRepeatedMetadataIdsAndExtraData() throws IOException {
        RevTree tree;
        List<ObjectId> repeatingMdIds = ImmutableList.of(//
                hashString("mdid1"), //
                hashString("mdid2"), //
                hashString("mdid3"));
        List<Node> tNodes = nodes(TYPE.TREE, 1024, repeatingMdIds, true, true);
        List<Node> fNodes = nodes(TYPE.FEATURE, 1024, repeatingMdIds, true, true);
        tree = tree(2048, tNodes, fNodes, null);
        encodeDecode(tree);
    }

    @Test
    public void testBucketsTree() throws IOException {
        RevTree tree;
        SortedMap<Integer, Bucket> buckets = new TreeMap<>();
        buckets.put(1, Bucket.create(hashString("b1"), null));
        tree = tree(1024, null, null, buckets);
        encodeDecode(tree);
    }

    private RevTree encodeDecode(RevTree orig) throws IOException {
        final byte[] encoded = RevTreeFormat.encode(orig);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Stopwatch s;

        {
            s = Stopwatch.createStarted();
            int repeatCount = 100;
            try {
                for (int i = 0; i < repeatCount; i++) {
                    out.reset();
                    DataStreamSerializationFactoryV2.INSTANCE.write(orig, out);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.err.printf("V2 encoding: %s\n", s.stop());

            byte[] v2data = out.toByteArray();
            s.reset().start();
            for (int i = 0; i < repeatCount; i++) {
                RevTree d = (RevTree) DataStreamSerializationFactoryV2.INSTANCE
                        .read(RevTree.EMPTY_TREE_ID, new ByteArrayInputStream(v2data));
                for (Node n : Iterables.concat(d.trees(), d.features())) {
                    n.getName();
                    n.getObjectId();
                    n.getMetadataId();
                    n.getExtraData();
                }
            }
            System.err.printf("V2 decoding: %s\n", s.stop());

            s = Stopwatch.createStarted();
            for (int i = 0; i < repeatCount; i++) {
                out.reset();
                RevTreeFormat.encode(orig, out);
            }
            System.err.printf("V3 encoding: %s\n", s.stop());
            s.reset().start();
            for (int i = 0; i < repeatCount; i++) {
                RevTree d = RevTreeFormat.decode(RevTree.EMPTY_TREE_ID, encoded);
                for (Node n : Iterables.concat(d.trees(), d.features())) {
                    n.getName();
                    n.getObjectId();
                    n.getMetadataId();
                    n.getExtraData();
                }
            }
            System.err.printf("V3 decoding: %s\n", s.stop());
        }
        {
            out = new ByteArrayOutputStream();
            try {
                DataStreamSerializationFactoryV2.INSTANCE.write(orig, out);
                ByteArrayOutputStream v2compressed = compress(out.toByteArray());
                ByteArrayOutputStream v3compressed = compress(encoded);
                System.err.printf(
                        "tree size: %,d, bytes: V3=%,d, V2=%,d. compressed: V3=%,d; V2=%,d\n",
                        orig.size(), encoded.length, out.size(), v3compressed.size(),
                        v2compressed.size());

            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        assertNotNull(encoded);
        RevTree decoded = RevTreeFormat.decode(orig.getId(), encoded);
        assertEquals(orig, decoded);
        // equals only checks for objectId

        assertEquals(TYPE.TREE, decoded.getType());
        assertEquals(orig.size(), decoded.size());
        assertEquals(orig.numTrees(), decoded.numTrees());
        assertEqualsFully(orig.trees(), decoded.trees());
        assertEqualsFully(orig.features(), decoded.features());
        assertEquals(orig.buckets(), decoded.buckets());

        return decoded;
    }

    private ByteArrayOutputStream compress(byte[] bs) throws IOException {
        ByteArrayOutputStream c = new ByteArrayOutputStream();
        OutputStream compressingOut = new LZFOutputStream(c);
        compressingOut.write(bs);
        compressingOut.close();
        return c;
    }
}
