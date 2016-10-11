package org.locationtech.geogig.geotools.zip;

import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.geotools.data.DataStore;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.geogig.api.ProgressListener;
import org.locationtech.geogig.api.RevFeatureType;
import org.locationtech.geogig.api.plumbing.ResolveFeatureType;
import org.locationtech.geogig.geotools.plumbing.DataStoreExportOp;
import org.locationtech.geogig.geotools.plumbing.ExportOp;
import org.opengis.feature.Feature;
import org.opengis.feature.GeometryAttribute;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.vividsolutions.jts.geom.Geometry;

public class ZipDataStoreExportOp extends DataStoreExportOp<File> {

    private File shape;

    private Boolean doitt;
    
    //private List<String> removeFields = Arrays.asList("fromTocl", "toFromcl", "fromToNode",
     //       "toFromNode", "fromToHR", "toFromHR");
    private List<String> removeFields = Arrays.asList("fromTocl", "toFromcl");

    // private List<String> removeFields = Arrays.asList("note");
    public ZipDataStoreExportOp setShapeFile(File shape) {
        this.shape = shape;
        return this;
    }

    public ZipDataStoreExportOp setDoittImport(boolean enable) {
        this.doitt = enable;
        return this;
    }

    private class FromTo {
        private String toFromClass;

        private String fromToClass;

        public String getToFromClass() {
            return toFromClass;
        }

        public void setToFromClass(String toFromClass) {
            this.toFromClass = toFromClass;
        }

        public String getFromToClass() {
            return fromToClass;
        }

        public void setFromToClass(String fromToClass) {
            this.fromToClass = fromToClass;
        }

    }

    @Override
    protected void export(final String treeSpec, final DataStore targetStore,
            final String targetTableName, final ProgressListener progress) {

        if (this.doitt) {

            Optional<RevFeatureType> opType = command(ResolveFeatureType.class)
                    .setRefSpec(treeSpec).call();
            checkState(opType.isPresent());

            SimpleFeatureType featureType = (SimpleFeatureType) opType.get().type();
            CoordinateReferenceSystem worldCRS = getTargetCRS();
            CoordinateReferenceSystem dataCRS = featureType.getCoordinateReferenceSystem();
            SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();

            for (AttributeDescriptor descriptor : featureType.getAttributeDescriptors()) {
                if (!removeFields.contains(descriptor.getLocalName()))
                    builder.add(descriptor);
            }
            builder.add("BikeLane", String.class);
            builder.setName(featureType.getName());
            builder.setCRS(worldCRS);
            SimpleFeatureType typeWithoutRemoved = builder.buildFeatureType();


            SimpleFeatureType reprojFeatureType = SimpleFeatureTypeBuilder.retype(
                    typeWithoutRemoved,
                    worldCRS);



            try {
                targetStore.createSchema(reprojFeatureType);
            } catch (IOException e) {
                throw new IllegalStateException("Unable to create feature type from " + treeSpec);
            }

            SimpleFeatureSource featureSource;
            try {
                featureSource = targetStore.getFeatureSource(featureType.getName().getLocalPart());
            } catch (IOException e) {
                throw new IllegalStateException("Unable to obtain feature type once created: "
                        + treeSpec);
            }
            checkState(featureSource instanceof SimpleFeatureStore,
                    "FeatureSource is not writable: " + featureType.getName().getLocalPart());

            SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;

            SimpleFeatureBuilder fbuilder = new SimpleFeatureBuilder(reprojFeatureType);



            MathTransform transform;
            try {
                transform = CRS.findMathTransform(dataCRS, worldCRS, true);

                /*
                 * final Function<Feature, Optional<Feature>> function = (feature) -> {
                 * SimpleFeature simpleFeature = (SimpleFeature) feature;
                 * featureBuilder.add(simpleFeature.getAttribute(0));
                 * featureBuilder.add(simpleFeature.getAttribute(2)); return Optional.of((Feature)
                 * featureBuilder.buildFeature(null)); };
                 */
                final Function<Feature, Optional<Feature>> function = (feature) -> {
                    FromTo ft = new FromTo();

                    for (Property property : feature.getProperties()) {
                        if (property instanceof GeometryAttribute) {
                            Geometry geometry = (Geometry) property.getValue();
                            Geometry geometry2;
                            try {
                                geometry2 = JTS.transform(geometry, transform);
                                fbuilder.set(featureType.getGeometryDescriptor().getName(),
                                        geometry2);
                            } catch (MismatchedDimensionException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            } catch (TransformException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }

                        } else if (removeFields.contains(property.getName().toString())) {
                            if (property.getName().toString().equalsIgnoreCase("fromTocl")) {
                                if (property.getValue() != null && !property.getValue().equals("")) {
                                    ft.setFromToClass((String) property.getValue());
                                }

                            } else if (property.getName().toString().equalsIgnoreCase("toFromcl")) {
                                if (property.getValue() != null && !property.getValue().equals("")) {
                                    ft.setToFromClass((String) property.getValue());
                                }

                            }

                        }else {

                            fbuilder.set(property.getName(), property.getValue());
                        }
                    }
                    fbuilder.set("BikeLane", mergeClasses(ft));
                    if (feature.getProperty("BikeDir").getValue() == null || feature.getProperty("BikeDir").getValue().equals("")) 
                    	fbuilder.set("BikeDir", bikeDirection(ft));
                    Feature modifiedFeature = fbuilder
                            .buildFeature(feature.getIdentifier().getID());

                    return Optional.fromNullable(modifiedFeature);
                };

                command(ExportOp.class)//
                        .setFeatureStore(featureStore)//
                        .setPath(treeSpec)//
                        .exportDefaultFeatureType().setAlter(true)
                        .setFeatureTypeConversionFunction(function).setTransactional(true)//
                        .setBBoxFilter(bboxFilter)//
                        .setProgressListener(progress)//
                        .call();

            } catch (FactoryException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        } else {
            super.export(treeSpec, targetStore, targetTableName, progress);
        }

    }

/*    private String mergeClasses(FromTo ft) {
        if (ft.getFromToClass() != null && ft.getToFromClass() != null)
            return ft.getFromToClass() + "," + ft.getToFromClass();
        else if (ft.getFromToClass() != null)
            return ft.getFromToClass();
        else if (ft.getToFromClass() != null)
            return ft.getToFromClass();
        else
            return "";
    }*/
    private String mergeClasses(FromTo ft) {
    	if(ft.getFromToClass()!=null&&ft.getToFromClass()!=null){
	    	if(ft.getFromToClass().equalsIgnoreCase(ft.getToFromClass())){
	    		return ft.getFromToClass();
	    	}
	    	else{
	    		if(ft.getFromToClass().equals("1")&&ft.getToFromClass().equals("2"))
	    			return "5";
	    		else if (ft.getFromToClass().equals("2")&&ft.getToFromClass().equals("3"))
	    			return "6";
	    		else if (ft.getFromToClass().equals("1")&&ft.getToFromClass().equals("3"))
	    			return "8";
	    		else if (ft.getFromToClass().equals("2")&&ft.getToFromClass().equals("1"))
	    			return "9";
	    		else if (ft.getFromToClass().equals("3")&&ft.getToFromClass().equals("1"))
	    			return "10";
	    		else if (ft.getFromToClass().equals("3")&&ft.getToFromClass().equals("2"))
	    			return "11";
	    		else if(!ft.getFromToClass().equals("")){
	    			return ft.getFromToClass();
	    		}else if(ft.getToFromClass().equals("")){
	    			return ft.getToFromClass();
	    		}
	    	}
    	}else{
    		if(ft.getFromToClass()!=null){
    			return ft.getFromToClass();
    		}else if(ft.getToFromClass()!=null){
    			return ft.getToFromClass();
    		}
    	}
    	return "";

    }
    
    private String bikeDirection(FromTo ft){
    	if(ft.getFromToClass()!=null&&!ft.getFromToClass().equals("")&&ft.getToFromClass()!=null&&!ft.getToFromClass().equals("")){
    		return "Two-way";
    	}else if(ft.getFromToClass()!=null&&ft.getFromToClass()!=""){
    		return "With";
    	}else if(ft.getToFromClass()!=null&&ft.getToFromClass()!=""){
    		return "Against";
    	}else{
    		return "";
    	}
    }

    private CoordinateReferenceSystem getTargetCRS() {
        CoordinateReferenceSystem crsout = null;
        try {
            crsout = CRS.decode("EPSG:2263");
        } catch (NoSuchAuthorityCodeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (FactoryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return crsout;
    }

    @Override
    protected File buildResult(DataStore targetStore) {
        Path zipfile = null;
        Path temppath2;
        try {
            temppath2 = Files.createTempDirectory("geogigshapeexport");
            zipfile = Files.createTempFile(temppath2, "geogigshapeexport", ".zip");
            FileOutputStream fos = new FileOutputStream(zipfile.toString());
            ZipOutputStream zip = new ZipOutputStream(fos);
            zipDirectory(shape.getParentFile(), zip);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (zipfile != null)
            return zipfile.toFile();
        else
            return null;
    }

    private void zipDirectory(File shppath, ZipOutputStream zip) throws IOException,
            FileNotFoundException {
        File[] shpdirfiles = shppath.listFiles();
        for (int i = 0; i < shpdirfiles.length; i++) {
            byte[] buffer = new byte[1024];
            File file = shpdirfiles[i];
            ZipEntry e = new ZipEntry(file.getName());
            zip.putNextEntry(e);
            FileInputStream fis = new FileInputStream(file);
            int length;
            while ((length = fis.read(buffer)) > 0) {
                zip.write(buffer, 0, length);
            }
            zip.closeEntry();
            fis.close();
        }

        zip.finish();
        zip.flush();
        zip.close();
    }
}
