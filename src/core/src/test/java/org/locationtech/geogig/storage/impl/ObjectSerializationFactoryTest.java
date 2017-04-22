/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.locationtech.geogig.model.impl.RevFeatureBuilder.build;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.UUID;

import org.geotools.data.DataUtilities;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.WKTReader2;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geogig.model.Bounded;
import org.locationtech.geogig.model.Bucket;
import org.locationtech.geogig.model.CanonicalNodeOrder;
import org.locationtech.geogig.model.Node;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.model.RevFeature;
import org.locationtech.geogig.model.RevFeatureType;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.model.RevObject.TYPE;
import org.locationtech.geogig.model.RevObjects;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.model.impl.CommitBuilder;
import org.locationtech.geogig.model.impl.RevFeatureBuilder;
import org.locationtech.geogig.model.impl.RevFeatureTypeBuilder;
import org.locationtech.geogig.model.impl.RevObjectTestSupport;
import org.locationtech.geogig.model.impl.RevTreeBuilder;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.GeometryDescriptor;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;

public abstract class ObjectSerializationFactoryTest {

    protected ObjectSerializingFactory serializer;

    private CommitBuilder testCommit;

    private String namespace = "http://geogig.org/test";

    private String typeName = "TestType";

    private String typeSpec1 = "str:String," + //
            "bool:Boolean," + //
            "byte:java.lang.Byte," + //
            "doub:Double," + //
            "bdec:java.math.BigDecimal," + //
            "flt:Float," + //
            "int:Integer," + //
            "bint:java.math.BigInteger," + //
            "pp:Point:srid=4326," + //
            "lng:java.lang.Long," + //
            "datetime:java.util.Date," + //
            "date:java.sql.Date," + //
            "time:java.sql.Time," + //
            "timestamp:java.sql.Timestamp," + //
            "uuid:java.util.UUID";

    protected SimpleFeatureType featureType1;

    protected Feature feature1_1;

    private String typeSpec = "str:String," + "bool:Boolean," + "byte:java.lang.Byte,"
            + "doub:Double," + "bdec:java.math.BigDecimal," + "flt:Float," + "int:Integer,"
            + "bint:java.math.BigInteger," + "pp:Point:srid=4326," + "lng:java.lang.Long,"
            + "uuid:java.util.UUID";

    private SimpleFeatureType featureType;

    private RevTree tree1_leaves;

    private RevTree tree2_internal;

    private RevTree tree3_buckets;

    private RevTree tree4_spatial_leaves;

    private RevTree tree5_spatial_internal;

    private RevTree tree6_spatial_buckets;

    @Before
    public void before() throws Exception {
        this.serializer = getObjectSerializingFactory();
        ObjectId treeId = RevObjectTestSupport.hashString("treeid");
        testCommit = testCommit(treeId, "groldan", "groldan@boundlessgeo.com", 5000L, "jd",
                "jd@lmnsolutions.com", 10000L, "test message",
                RevObjectTestSupport.hashString("first parent"));

        /* now we will setup our feature types and test features. */
        featureType1 = DataUtilities.createType(namespace, typeName, typeSpec1);
        // have to store timestamp in a variable since the nanos field is only accessible via setter
        // and getter
        java.sql.Timestamp timestamp = new java.sql.Timestamp(1264396155228L);
        timestamp.setNanos(23456);
        feature1_1 = feature(featureType1, //
                "TestType.feature.1", //
                "StringProp1_1", //
                Boolean.TRUE, //
                Byte.valueOf("18"), //
                new Double(100.01), //
                new BigDecimal("1.89e1021"), //
                new Float(12.5), //
                new Integer(1000), //
                new BigInteger("90000000"), //
                "POINT(1 1)", //
                new Long(800000), //
                new java.util.Date(1264396155228L), //
                new java.sql.Date(1364356800000L), //
                new java.sql.Time(57355228L), //
                timestamp, //
                UUID.fromString("bd882d24-0fe9-11e1-a736-03b3c0d0d06d"));
        featureType = DataUtilities.createType(namespace, typeName, typeSpec);

        ImmutableList<Node> features = ImmutableList.of(Node.create("foo",
                RevObjectTestSupport.hashString("nodeid"),
                RevObjectTestSupport.hashString("metadataid"), RevObject.TYPE.FEATURE, null));
        ImmutableList<Node> spatialFeatures = ImmutableList
                .of(Node.create("foo", RevObjectTestSupport.hashString("nodeid"),
                        RevObjectTestSupport.hashString("metadataid"), RevObject.TYPE.FEATURE,
                        new Envelope(0.0000001, 0.0000002, 0.0000001, 0.0000002)));
        ImmutableList<Node> trees = ImmutableList.of(Node.create("bar",
                RevObjectTestSupport.hashString("barnodeid"),
                RevObjectTestSupport.hashString("barmetadataid"), RevObject.TYPE.TREE, null));
        ImmutableList<Node> spatialTrees = ImmutableList
                .of(Node.create("bar", RevObjectTestSupport.hashString("barnodeid"),
                        RevObjectTestSupport.hashString("barmetadataid"), RevObject.TYPE.TREE,
                        new Envelope(1, 2, 1, 2)));

        SortedMap<Integer, Bucket> spatialBuckets = ImmutableSortedMap.of(1,
                Bucket.create(RevObjectTestSupport.hashString("buckettree"), new Envelope()));

        SortedMap<Integer, Bucket> buckets = ImmutableSortedMap.of(1, Bucket
                .create(RevObjectTestSupport.hashString("buckettree"), new Envelope(1, 2, 1, 2)));

        tree1_leaves = RevTreeBuilder.create(RevObjectTestSupport.hashString("leaves"), 1L, 0, null,
                features, null);
        tree2_internal = RevTreeBuilder.create(RevObjectTestSupport.hashString("internal"), 0,
                trees.size(), trees, null, null);
        tree3_buckets = RevTreeBuilder.create(RevObjectTestSupport.hashString("buckets"), 1L, 1,
                null, null, buckets);
        tree4_spatial_leaves = RevTreeBuilder.create(RevObjectTestSupport.hashString("leaves"), 1L,
                0, null, spatialFeatures, null);
        tree5_spatial_internal = RevTreeBuilder.create(RevObjectTestSupport.hashString("internal"),
                1L, spatialTrees.size(), spatialTrees, null, null);
        tree6_spatial_buckets = RevTreeBuilder.create(RevObjectTestSupport.hashString("buckets"),
                1L, 1, null, null, spatialBuckets);

    }

    protected abstract ObjectSerializingFactory getObjectSerializingFactory();

    @Test
    public void testCommitSerialization() throws IOException {
        RevCommit commit = testCommit.build();
        testCommit(commit);
    }

    @Test
    public void testCommitSerializationMultipleLinesMessage() throws IOException {
        testCommit.setMessage("this\n is a \n  multiple lines\n message");
        RevCommit commit = testCommit.build();
        testCommit(commit);
    }

    @Test
    public void testCommitSerializationNoAuthor() throws IOException {
        testCommit.setAuthor(null);
        testCommit.setAuthorEmail(null);
        RevCommit commit = testCommit.build();
        testCommit(commit);
    }

    @Test
    public void testCommitSerializationNoCommitter() throws IOException {
        testCommit.setCommitter(null);
        testCommit.setCommitterEmail(null);
        RevCommit commit = testCommit.build();
        testCommit(commit);
    }

    @Test
    public void testCommitSerializationNoMessage() throws IOException {
        testCommit.setMessage(null);
        RevCommit commit = testCommit.build();
        testCommit(commit);
    }

    @Test
    public void testCommitSerializationNoParents() throws IOException {
        testCommit.setParentIds(null);
        RevCommit commit = testCommit.build();
        testCommit(commit);
    }

    @Test
    public void testCommitSerializationMultipleParents() throws IOException {
        testCommit.setParentIds(ImmutableList.of(RevObjectTestSupport.hashString("parent1"),
                RevObjectTestSupport.hashString("parent2"),
                RevObjectTestSupport.hashString("parent3"),
                RevObjectTestSupport.hashString("parent4")));
        RevCommit commit = testCommit.build();
        testCommit(commit);
    }

    private void testCommit(RevCommit commit) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        serializer.write(commit, out);

        RevObject read = serializer.read(commit.getId(),
                new ByteArrayInputStream(out.toByteArray()));
        assertEquals(commit, read);
    }

    private CommitBuilder testCommit(ObjectId treeId, String author, String authorEmail,
            long authorTimestamp, String committer, String committerEmail, long committerTimestamp,
            String message, ObjectId... parentIds) {
        CommitBuilder b = new CommitBuilder();
        b.setTreeId(treeId);
        b.setAuthor(author);
        b.setAuthorEmail(authorEmail);
        b.setCommitter(committer);
        b.setCommitterEmail(committerEmail);
        b.setMessage(message);
        b.setAuthorTimestamp(authorTimestamp);
        b.setCommitterTimestamp(committerTimestamp);
        if (parentIds != null) {
            b.setParentIds(Lists.newArrayList(parentIds));
        }
        return b;
    }

    @Test
    public void testCommitRoundTrippin() throws Exception {
        long currentTime = System.currentTimeMillis();
        int timeZoneOffset = TimeZone.getDefault().getOffset(currentTime);
        CommitBuilder builder = new CommitBuilder();
        String author = "groldan";
        builder.setAuthor(author);
        String authorEmail = "groldan@boundlessgeo.com";
        builder.setAuthorEmail(authorEmail);
        builder.setAuthorTimestamp(currentTime);
        builder.setAuthorTimeZoneOffset(timeZoneOffset);
        String committer = "mleslie";
        builder.setCommitter(committer);
        String committerEmail = "mleslie@boundlessgeo.com";
        builder.setCommitterEmail(committerEmail);
        builder.setCommitterTimestamp(currentTime);
        builder.setCommitterTimeZoneOffset(timeZoneOffset);

        ObjectId treeId = RevObjectTestSupport.hashString("Fake tree");
        builder.setTreeId(treeId);

        ObjectId parent1 = RevObjectTestSupport.hashString("Parent 1 of fake commit");
        ObjectId parent2 = RevObjectTestSupport.hashString("Parent 2 of fake commit");
        List<ObjectId> parents = Arrays.asList(parent1, parent2);
        builder.setParentIds(parents);

        RevCommit cmtIn = builder.build();
        assertNotNull(cmtIn);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        serializer.write(cmtIn, bout);

        byte[] bytes = bout.toByteArray();
        assertTrue(bytes.length > 0);

        RevCommit cmtOut = (RevCommit) read(cmtIn.getId(), bytes);

        assertEquals(treeId, cmtOut.getTreeId());
        assertEquals(parents, cmtOut.getParentIds());
        assertEquals(author, cmtOut.getAuthor().getName().get());
        assertEquals(authorEmail, cmtOut.getAuthor().getEmail().get());
        assertEquals(committer, cmtOut.getCommitter().getName().get());
        assertEquals(committerEmail, cmtOut.getCommitter().getEmail().get());
        assertEquals(currentTime, cmtOut.getCommitter().getTimestamp());
        assertEquals(timeZoneOffset, cmtOut.getCommitter().getTimeZoneOffset());
        assertEquals(currentTime, cmtOut.getAuthor().getTimestamp());
        assertEquals(timeZoneOffset, cmtOut.getAuthor().getTimeZoneOffset());

    }

    @Test
    public void testSerializeFeature() throws Exception {
        testFeatureReadWrite(feature1_1);
    }

    protected void testFeatureReadWrite(Feature feature) throws Exception {

        RevFeature newFeature = RevFeatureBuilder.build(feature);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        serializer.write(newFeature, output);

        byte[] data = output.toByteArray();
        assertTrue(data.length > 0);

        ByteArrayInputStream input = new ByteArrayInputStream(data);
        RevFeature feat = (RevFeature) serializer.read(newFeature.getId(), input);

        assertNotNull(feat);
        assertEquals(newFeature.getValues().size(), feat.getValues().size());

        for (int i = 0; i < newFeature.getValues().size(); i++) {
            assertEquals(newFeature.getValues().get(i).orNull(), feat.getValues().get(i).orNull());
        }

    }

    private Geometry geom(String wkt) throws ParseException {
        Geometry value = new WKTReader2().read(wkt);
        return value;
    }

    protected Feature feature(SimpleFeatureType type, String id, Object... values)
            throws ParseException {
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(type);
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (type.getDescriptor(i) instanceof GeometryDescriptor) {
                if (value instanceof String) {
                    value = geom((String) value);
                }
            }
            builder.set(i, value);
        }
        return builder.buildFeature(id);
    }

    @Test
    public void testLargeStringValue() throws Exception {

        SimpleFeatureType type = DataUtilities.createType("LongStringType", "clob:String");

        final int length = 256 * 1024;
        final String largeString = Strings.repeat("a", length);

        Feature feature = feature(type, "fid1", largeString);

        RevFeature revFeature = RevFeatureBuilder.build(feature);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        serializer.write(revFeature, output);

        byte[] data = output.toByteArray();

        ByteArrayInputStream input = new ByteArrayInputStream(data);
        RevFeature feat = (RevFeature) serializer.read(revFeature.getId(), input);
        assertNotNull(feat);
        assertEquals(1, feat.getValues().size());

        Optional<Object> value = feat.getValues().get(0);
        assertTrue(value.isPresent());
        String deserialized = (String) value.get();

        assertEquals(largeString.length(), deserialized.length());
        assertEquals(largeString, deserialized);
    }

    @Test
    public void testFeatureMapAttribute() throws Exception {

        SimpleFeatureType featureType = DataUtilities.createType("http://geogig.org/test",
                "TestType", "str:String, map:java.util.Map");

        Map<String, Object> map1, map2, map3;
        map1 = new HashMap<>();
        map2 = new TreeMap<>();

        map1.put("long", Long.valueOf(123));
        map2.put("long", Long.valueOf(123));

        map1.put("int", Integer.valueOf(456));
        map2.put("int", Integer.valueOf(456));

        map1.put("string", "hello");
        map2.put("string", "hello");

        map1.put("geom", geom("LINESTRING(1 1, 1.1 2.1, 100 1000)"));
        map2.put("geom", geom("LINESTRING(1 1, 1.1 2.1, 100 1000)"));

        map3 = ImmutableMap.of("I", (Object) "am", "a", (Object) "different", "map than",
                (Object) map1, "and", (Object) map2);

        RevFeature revFeature1 = build(feature(featureType, "f1", "the name", map1));
        RevFeature revFeature2 = build(feature(featureType, "f2", "the name", map2));
        RevFeature revFeature3 = build(feature(featureType, "f3", "the name", map3));

        assertEquals(revFeature1, revFeature2);
        assertEquals(revFeature1.getValues(), revFeature2.getValues());

        byte[] data1 = serialize(revFeature1);
        byte[] data2 = serialize(revFeature2);
        byte[] data3 = serialize(revFeature3);

        RevFeature read1 = (RevFeature) read(revFeature1.getId(), data1);
        RevFeature read2 = (RevFeature) read(revFeature2.getId(), data2);
        RevFeature read3 = (RevFeature) read(revFeature3.getId(), data3);

        assertEquals(read1, read2);
        assertEquals(read1.getValues(), read2.getValues());
        assertEquals(revFeature3, read3);
        assertEquals(revFeature3.getValues(), read3.getValues());
    }

    @Test
    public void testTreeNodesExtraData() throws Exception {
        Map<String, Object> map1, map2, extraData;
        map1 = new HashMap<>();
        map2 = new TreeMap<>();

        map1.put("long", Long.valueOf(123));
        map2.put("long", Long.valueOf(123));

        map1.put("int", Integer.valueOf(456));
        map2.put("int", Integer.valueOf(456));

        map1.put("string", "hello");
        map2.put("string", "hello");

        map1.put("geom", geom("LINESTRING(1 1, 1.1 2.1, 100 1000)"));
        map2.put("geom", geom("LINESTRING(1 1, 1.1 2.1, 100 1000)"));

        extraData = ImmutableMap.of("I", (Object) "am", "a", (Object) "different", "map than",
                (Object) map1, "and", (Object) map2);

        Node n = Node.create("fid", RevObjectTestSupport.hashString("id"), ObjectId.NULL,
                TYPE.FEATURE, null, extraData);

        RevTree tree = RevTreeBuilder.build(1, 0, null, ImmutableList.of(n), null);

        RevObject roundTripped = read(tree.getId(), write(tree));

        assertEqualsFully(tree, roundTripped);
    }

    private byte[] serialize(RevFeature revFeature1) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        serializer.write(revFeature1, output);

        byte[] data = output.toByteArray();
        return data;
    }

    @Test
    public void testFeatureTypeSerialization() throws Exception {
        RevFeatureType revFeatureType = RevFeatureTypeBuilder.build(featureType);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        serializer.write(revFeatureType, output);

        byte[] data = output.toByteArray();
        assertTrue(data.length > 0);

        ByteArrayInputStream input = new ByteArrayInputStream(data);
        RevFeatureType rft = (RevFeatureType) serializer.read(revFeatureType.getId(), input);

        assertNotNull(rft);
        SimpleFeatureType serializedFeatureType = (SimpleFeatureType) rft.type();
        assertEquals(serializedFeatureType.getDescriptors().size(),
                featureType.getDescriptors().size());

        for (int i = 0; i < featureType.getDescriptors().size(); i++) {
            assertEquals(featureType.getDescriptor(i), serializedFeatureType.getDescriptor(i));
        }

        assertEquals(featureType.getGeometryDescriptor(),
                serializedFeatureType.getGeometryDescriptor());
        assertEquals(featureType.getCoordinateReferenceSystem(),
                serializedFeatureType.getCoordinateReferenceSystem());
    }

    @Test
    public void testFeatureTypeSerializationWGS84() throws Exception {
        SimpleFeatureTypeBuilder ftb = new SimpleFeatureTypeBuilder();
        ftb.add("geom", Polygon.class, DefaultGeographicCRS.WGS84);
        ftb.setName("type");
        SimpleFeatureType ftype = ftb.buildFeatureType();
        RevFeatureType revFeatureType = RevFeatureTypeBuilder.build(ftype);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        serializer.write(revFeatureType, output);

        byte[] data = output.toByteArray();
        assertTrue(data.length > 0);

        ByteArrayInputStream input = new ByteArrayInputStream(data);
        RevFeatureType rft = (RevFeatureType) serializer.read(revFeatureType.getId(), input);

        assertNotNull(rft);
        FeatureType serializedFeatureType = rft.type();

        assertEquals("EPSG:4326", CRS.toSRS(serializedFeatureType.getCoordinateReferenceSystem()));

    }

    @Test
    public void testRoundTripLeafTree() throws IOException {
        RevTree roundTripped = (RevTree) read(tree1_leaves.getId(), write(tree1_leaves));
        assertTreesAreEqual(tree1_leaves, roundTripped);
    }

    @Test
    public void testRoundTripInternalTree() throws IOException {
        RevTree roundTripped = (RevTree) read(tree2_internal.getId(), write(tree2_internal));
        assertTreesAreEqual(tree2_internal, roundTripped);
    }

    @Test
    public void testRoundTripBuckets() throws IOException {
        RevTree roundTripped = (RevTree) read(tree3_buckets.getId(), write(tree3_buckets));
        assertTreesAreEqual(tree3_buckets, roundTripped);
    }

    @Test
    public void testRoundTripBucketsFull() throws IOException {

        ObjectId id = RevObjectTestSupport.hashString("fake");
        long size = 100000000;
        int childTreeCount = 0;
        SortedMap<Integer, Bucket> bucketTrees = createBuckets(32);

        final RevTree tree = RevTreeBuilder.create(id, size, childTreeCount, null, null,
                bucketTrees);

        RevTree roundTripped = (RevTree) read(tree.getId(), write(tree));
        assertTreesAreEqual(tree, roundTripped);

    }

    private SortedMap<Integer, Bucket> createBuckets(int count) {
        SortedMap<Integer, Bucket> buckets = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            Bucket bucket = Bucket.create(RevObjectTestSupport.hashString("b" + i),
                    new Envelope(i, i * 2, i, i * 2));
            buckets.put(i, bucket);
        }
        return buckets;
    }

    @Test
    public void testRoundTripSpatialLeafTree() throws IOException {
        RevTree roundTripped = (RevTree) read(tree4_spatial_leaves.getId(),
                write(tree4_spatial_leaves));
        assertTreesAreEqual(tree4_spatial_leaves, roundTripped);
    }

    @Test
    public void testRoundTripSpatialInternalTree() throws IOException {
        RevTree roundTripped = (RevTree) read(tree5_spatial_internal.getId(),
                write(tree5_spatial_internal));
        assertTreesAreEqual(tree5_spatial_internal, roundTripped);
    }

    @Test
    public void testRoundTripSpatialBuckets() throws IOException {
        RevTree roundTripped = (RevTree) read(tree6_spatial_buckets.getId(),
                write(tree6_spatial_buckets));
        assertTreesAreEqual(tree6_spatial_buckets, roundTripped);
    }

    private byte[] write(RevTree tree) {
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            serializer.write(tree, bout);
            return bout.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private RevObject read(ObjectId id, byte[] bytes) throws IOException {
        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
        RevObject obj = serializer.read(id, bin);
        RevObject obj2 = serializer.read(id, bytes, 0, bytes.length);
        assertEquals(obj, obj2);
        assertEqualsFully(obj, obj2);
        return obj;
    }

    private void assertEqualsFully(RevObject o1, RevObject o2) {
        if (o1 instanceof RevTree) {
            assertEqualsFully((RevTree) o1, (RevTree) o2);
        }

    }

    private void assertEqualsFully(RevTree o1, RevTree o2) {
        assertEquals(o1.size(), o2.size());
        assertEquals(o1.numTrees(), o2.numTrees());
        assertNodesEqual(o1.features(), o2.features());
        assertNodesEqual(o1.trees(), o2.trees());
        assertEquals(o1.buckets(), o2.buckets());
    }

    private void assertNodesEqual(List<Node> l1, List<Node> l2) {
        assertEquals(l1, l2);
        for (int i = 0; i < l1.size(); i++) {
            Node n1 = l1.get(i);
            Node n2 = l2.get(i);
            assertEquals(n1.getExtraData(), n2.getExtraData());
            assertEquals(n1.getMetadataId(), n2.getMetadataId());
            assertEquals(n1.bounds(), n2.bounds());
        }
    }

    public void assertTreesAreEqual(RevTree a, RevTree b) {
        assertEquals(a.getId(), b.getId());
        assertEquals(a.buckets(), b.buckets());
        assertEquals(a.features(), b.features());
        assertEquals(a.trees(), b.trees());
        assertEquals(a.numTrees(), b.numTrees());
        assertEquals(a.size(), b.size());

        Iterator<? extends Bounded> ia;
        Iterator<? extends Bounded> ib;
        if (a.buckets().isEmpty()) {
            ia = RevObjects.children(a, CanonicalNodeOrder.INSTANCE);
            ib = RevObjects.children(b, CanonicalNodeOrder.INSTANCE);
        } else {
            ia = a.buckets().values().iterator();
            ib = b.buckets().values().iterator();
        }

        // bounds are not part of the Bounded.equals(Object) contract since it's auxiliary
        // information
        while (ia.hasNext()) {
            Bounded ba = ia.next();
            Bounded bb = ib.next();
            Envelope ea = new Envelope();
            Envelope eb = new Envelope();
            ba.expand(ea);
            bb.expand(eb);
            assertEquals(ea.getMinX(), eb.getMinX(), 1e-7D);
            assertEquals(ea.getMinY(), eb.getMinY(), 1e-7D);
            assertEquals(ea.getMaxX(), eb.getMaxX(), 1e-7D);
            assertEquals(ea.getMaxY(), eb.getMaxY(), 1e-7D);
        }
    }
}
