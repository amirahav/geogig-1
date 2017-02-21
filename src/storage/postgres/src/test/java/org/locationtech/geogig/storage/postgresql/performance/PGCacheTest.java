package org.locationtech.geogig.storage.postgresql.performance;

import static org.locationtech.geogig.model.impl.RevObjectTestSupport.featureForceId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.locationtech.geogig.model.Bucket;
import org.locationtech.geogig.model.Node;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevFeature;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.model.RevObject.TYPE;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.model.impl.RevObjectTestSupport;
import org.locationtech.geogig.model.impl.RevTreeBuilder;
import org.locationtech.geogig.repository.IndexInfo;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV1;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV2;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV2_1;
import org.locationtech.geogig.storage.datastream.LZ4SerializationFactory;
import org.locationtech.geogig.storage.datastream.LZFSerializationFactory;
import org.locationtech.geogig.storage.datastream.v2_2.DataStreamSerializationFactoryV2_2;
import org.locationtech.geogig.storage.impl.ObjectSerializingFactory;
import org.locationtech.geogig.storage.postgresql.PGCache;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;

public class PGCacheTest {

    final int featureCount = 1000_000;

    final int treeCount = 10_000;

    private PGCache cache;

    List<RevFeature> features;// = createFeatures(ids);

    private List<RevTree> leafTrees;

    private List<RevTree> bucketTrees;

    private static final List<ObjectSerializingFactory> encoders = ImmutableList.of(//
            // raw encoders
            DataStreamSerializationFactoryV1.INSTANCE //
            , DataStreamSerializationFactoryV2.INSTANCE //
            , DataStreamSerializationFactoryV2_1.INSTANCE//
            , DataStreamSerializationFactoryV2_2.INSTANCE//
            // LZF encoders
            , new LZFSerializationFactory(DataStreamSerializationFactoryV1.INSTANCE)//
            , new LZFSerializationFactory(DataStreamSerializationFactoryV2.INSTANCE)//
            , new LZFSerializationFactory(DataStreamSerializationFactoryV2_1.INSTANCE)//
            , new LZFSerializationFactory(DataStreamSerializationFactoryV2_2.INSTANCE)//
            // LZ4 encoders
            , new LZ4SerializationFactory(DataStreamSerializationFactoryV1.INSTANCE)//
            , new LZ4SerializationFactory(DataStreamSerializationFactoryV2.INSTANCE)//
            , new LZ4SerializationFactory(DataStreamSerializationFactoryV2_1.INSTANCE)//
            , new LZ4SerializationFactory(DataStreamSerializationFactoryV2_2.INSTANCE)//
    );

    public static void main(String[] args) {
        PGCacheTest test = new PGCacheTest();
        System.err.println("set up...");
        test.setUp();

        final int runCount = 2;

        System.err.println("Leaf Trees test:");
        for (int i = 1; i <= runCount; i++) {
            test.runTest(test.leafTrees);
        }
        System.err.println(test.cache);
        test.tearDown();

        System.err.println("Bucket Trees test:");
        for (int i = 1; i <= runCount; i++) {
            test.runTest(test.bucketTrees);
        }
        System.err.println(test.cache);
        test.tearDown();

        System.err.println("Features test:");
        for (int i = 1; i <= runCount; i++) {
            test.runTest(test.features);
        }
        System.err.println(test.cache);
        test.tearDown();
    }

    public void setUp() {
        cache = PGCache.build();
        features = createFeatures(featureCount);
        leafTrees = createLeafTrees(treeCount);
        bucketTrees = createBucketTrees(treeCount);
    }

    public void tearDown() {
        cache.dispose();
    }

    public void runTest(List<? extends RevObject> objects) {
        System.err.println(
                "----------------------------------------------------------------------------------");
        System.err.printf("Format\t\t Count\t Hits\t Insert\t\t Query\t\t Size\t\t Stats\n");
        for (ObjectSerializingFactory encoder : encoders) {
            cache.invalidateAll();
            run(encoder, objects);
        }
    }

    public void run(ObjectSerializingFactory encoder, List<? extends RevObject> objects) {
        cache.setEncoder(encoder);
        final Stopwatch put = put(objects);

        Collections.shuffle(objects);
        final Stopwatch get = Stopwatch.createStarted();
        int hits = query(Lists.transform(objects, (f) -> f.getId()));
        get.stop();
        System.err.printf("%s\t %,d\t %,d\t %s\t %s\t %,d\t %s\n", encoder.getDisplayName(),
                objects.size(), hits, put, get, cache.sizeBytes(), ""/* cache.toString() */);
    }

    private int query(List<ObjectId> ids) {
        int hits = 0;
        for (ObjectId id : ids) {
            RevObject object = cache.getIfPresent(id);
            if (object != null) {
                hits++;
            }
        }
        return hits;
    }

    private Stopwatch put(List<? extends RevObject> objects) {
        Stopwatch sw = Stopwatch.createStarted();
        for (RevObject f : objects) {
            cache.put(f);
        }
        return sw.stop();
    }

    private List<RevTree> createLeafTrees(int count) {
        List<RevTree> trees = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            RevTree tree = createLeafTree(i);
            trees.add(tree);
        }
        return trees;
    }

    private List<RevTree> createBucketTrees(int count) {
        List<RevTree> trees = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            RevTree tree = createBucketTree(i);
            trees.add(tree);
        }
        return trees;
    }

    private RevTree createBucketTree(int i) {
        final int bucketCount = 32;
        SortedMap<Integer, Bucket> buckets = new TreeMap<>();
        for (int b = 0; b < bucketCount; b++) {
            ObjectId bucketTree = RevObjectTestSupport.hashString("b" + b);
            Envelope bounds = new Envelope(0, b, 0, b);
            Bucket bucket = Bucket.create(bucketTree, bounds);
            buckets.put(b, bucket);
        }
        final ObjectId fakeId = RevObjectTestSupport.hashString(String.valueOf(i));
        RevTree tree = RevTreeBuilder.create(fakeId, 1024, 0, null, null, buckets);
        return tree;
    }

    private RevTree createLeafTree(int i) {
        final int numNodes = 512;
        List<Node> nodes = new ArrayList<>(numNodes);
        for (int n = 0; n < numNodes; n++) {
            Node node = createNodeWithMetadata(n);
            nodes.add(node);
        }

        ObjectId id = RevObjectTestSupport.hashString("fake-tree-" + i);
        RevTree tree = RevTreeBuilder.create(id, numNodes, 0, null, ImmutableList.copyOf(nodes),
                null);
        return tree;
    }

    private Node createNodeWithMetadata(int n) {
        Map<String, Object> extraData = new HashMap<>();
        extraData.put("some-root-key", "some-string-value-" + n);
        Map<String, Object> extraAtts = new HashMap<>();
        extraData.put(IndexInfo.FEATURE_ATTRIBUTES_EXTRA_DATA, extraAtts);
        extraAtts.put("string-key", "String-value-" + n);
        extraAtts.put("integer-key", n);
        extraAtts.put("a-very-large-key-name-just-for-testing-result-size", "large-key-value-" + n);

        String name = "Node-" + n;
        ObjectId oid = RevObjectTestSupport.hashString(name);
        Envelope bounds = new Envelope(-1 * n, n, -1 * n, n);
        return Node.create(name, oid, ObjectId.NULL, TYPE.FEATURE, bounds, extraData);
    }

    private List<RevFeature> createFeatures(int count) {
        List<RevFeature> features = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ObjectId id = fakeId(i);
            features.add(fakeFeature(id));
        }
        return features;
    }

    private Geometry fakeGeom;

    private RevFeature fakeFeature(ObjectId forcedId) {
        // String oidString = objectId.toString();
        // ObjectId treeId = ObjectId.forString("tree" + oidString);
        // ImmutableList<ObjectId> parentIds = ImmutableList.of();
        // RevPerson author = new RevPersonImpl("Gabriel", "groldan@boundlessgeo.com", 1000, -3);
        // RevPerson committer = new RevPersonImpl("Gabriel", "groldan@boundlessgeo.com", 1000, -3);
        // String message = "message " + oidString;
        // return new RevCommitImpl(objectId, treeId, parentIds, author, committer, message);

        if (fakeGeom == null) {
            try {
                fakeGeom = new WKTReader().read(
                        "MULTIPOLYGON (((-121.3647138 38.049474, -121.3646902 38.049614, -121.3646159 38.0496058, -121.3646188 38.049587, -121.3645936 38.049586, -121.3645924 38.0496222, -121.3645056 38.0496178, -121.3645321 38.0494567, -121.3647138 38.049474)))");
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        return featureForceId(forcedId, fakeGeom, "Some string value " + forcedId);
    }

    private ObjectId fakeId(int i) {
        return RevObjectTestSupport.hashString("fakeID" + i);
    }
}
