package org.locationtech.geogig.rest.zip;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.io.FileUtils;
import org.geotools.data.DataStore;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.locationtech.geogig.repository.AbstractGeoGigOp;
import org.locationtech.geogig.repository.Context;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.plumbing.FindCommonAncestor;
import org.locationtech.geogig.plumbing.merge.MergeScenarioReport;
import org.locationtech.geogig.plumbing.merge.ReportMergeScenarioOp;
import org.locationtech.geogig.porcelain.MergeConflictsException;
import org.locationtech.geogig.geotools.geopkg.GeopkgMergeConflictsException;
import org.locationtech.geogig.geotools.plumbing.DataStoreImportOp;
import org.locationtech.geogig.geotools.plumbing.DataStoreImportOp.DataStoreSupplier;
import org.locationtech.geogig.geotools.plumbing.DefaultDataStoreImportOp;
import org.locationtech.geogig.rest.AsyncCommandRepresentation;
import org.locationtech.geogig.rest.AsyncContext.AsyncCommand;
import org.locationtech.geogig.rest.CommandRepresentationFactory;
import org.locationtech.geogig.rest.geotools.DataStoreImportContextService;
import org.locationtech.geogig.rest.repository.UploadCommandResource;
import org.locationtech.geogig.web.api.CommandSpecException;
import org.locationtech.geogig.web.api.PagedMergeScenarioConsumer;
import org.locationtech.geogig.web.api.ParameterSet;
import org.locationtech.geogig.web.api.ResponseWriter;
import org.locationtech.geogig.web.api.StreamWriterException;
import org.locationtech.geogig.web.api.StreamingWriter;
import org.restlet.data.MediaType;

import com.google.common.base.Optional;

public class ZipShpImportContext implements DataStoreImportContextService {


    private static final String SUPPORTED_FORMAT = "zip";

    private DataStoreSupplier dataStoreSupplier;

    @Override
    public boolean accepts(String format) {
        return SUPPORTED_FORMAT.equals(format);
    }

    @Override
    public DataStoreSupplier getDataStore(ParameterSet options) {
        if (dataStoreSupplier == null) {
            dataStoreSupplier = new ZipShpDataStoreSupplier(options);
        }
        return dataStoreSupplier;
    }

    @Override
    public String getCommandDescription() {
        return "Importing zipped Shapefile.";
    }

    @Override
    public DataStoreImportOp<RevCommit> createCommand(Context context, ParameterSet options) {
        return context.command(DefaultDataStoreImportOp.class);
    }

    private static class ZipShpDataStoreSupplier implements DataStoreSupplier {

        private ShapefileDataStore dataStore;

        private final ParameterSet options;

        private final File uploadedFile;

        private Path temppath;

        ZipShpDataStoreSupplier(ParameterSet options) {
            super();
            this.options = options;
            this.uploadedFile = options.getUploadedFile();
        }

        @Override
        public DataStore get() {
            if (null == dataStore) {
                // build one
                createDataStore();
            }
            return dataStore;
        }

        private void createDataStore() {
            final ShapefileDataStoreFactory factory = new ShapefileDataStoreFactory();
            final HashMap<String, Serializable> params = new HashMap<>(3);
            if (uploadedFile == null) {
                throw new CommandSpecException("Request must specify one and only one "
                        + UploadCommandResource.UPLOAD_FILE_KEY + " in the request body");
            }
            try {
                temppath = Files.createTempDirectory("zipshp");
                URL unzippedShp = unzipShapeFile(uploadedFile, temppath);
                // fill in DataStore parameters
                // params.put(ShapefileDataStoreFactory.URLP.key, uploadedFile.toURI().toURL());
                if (null == unzippedShp) {
                    throw new CommandSpecException(
                            "No file with a .shp extension was found in the zip");
                }
                params.put(ShapefileDataStoreFactory.URLP.key, unzippedShp);
                params.put(ShapefileDataStoreFactory.CREATE_SPATIAL_INDEX.key, Boolean.FALSE);
                params.put(ShapefileDataStoreFactory.ENABLE_SPATIAL_INDEX.key, Boolean.FALSE);

                dataStore = (ShapefileDataStore) factory.createDataStore(params);
            } catch (IOException ioe) {
                throw new CommandSpecException("Unable to create ShapefileDataStore: "
                        + ioe.getMessage());
            }
            if (null == dataStore) {
                throw new CommandSpecException(
                        "Unable to create ShapefileDataStore from uploaded file.");
            }
        }

        private URL unzipShapeFile(File zipFile, Path temppath) throws IOException {
            URL out = null;
            byte[] buffer = new byte[1024];
            ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile));
            ZipEntry ze = zis.getNextEntry();
            while (ze != null) {

                String fileName = ze.getName();
                File newFile = new File(temppath + File.separator + fileName);

                if ("shp".equalsIgnoreCase(com.google.common.io.Files.getFileExtension(newFile
                        .getAbsolutePath()))) {
                    out = newFile.toURI().toURL();
                }

                System.out.println("file unzip : " + newFile.getAbsoluteFile());

                // create all non exists folders
                // else you will hit FileNotFoundException for compressed folder
                new File(newFile.getParent()).mkdirs();

                FileOutputStream fos = new FileOutputStream(newFile);

                int len;
                while ((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }

                fos.close();
                ze = zis.getNextEntry();
            }

            zis.closeEntry();
            zis.close();

            return out;

        }

        @Override
        public void cleanupResources() {
            if (uploadedFile != null) {
                uploadedFile.delete();
            }

            if (temppath.toFile() != null) {
                try {
                    FileUtils.cleanDirectory(temppath.toFile());
                    temppath.toFile().delete();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }



        public static class RepresentationFactory implements
                CommandRepresentationFactory<RevCommit> {

            @Override
            public boolean supports(Class<? extends AbstractGeoGigOp<?>> cmdClass) {
                return DataStoreImportOp.class.equals(cmdClass);
            }

            @Override
            public AsyncCommandRepresentation<RevCommit> newRepresentation(
                    AsyncCommand<RevCommit> cmd, MediaType mediaType, String baseURL, boolean cleanup) {

                return new ZipShpImportRepresentation(mediaType, cmd, baseURL, cleanup);
            }
        }

        public static class ZipShpImportRepresentation extends
                AsyncCommandRepresentation<RevCommit> {

            public ZipShpImportRepresentation(MediaType mediaType,
                    AsyncCommand<RevCommit> cmd, String baseURL, boolean cleanup) {
                super(mediaType, cmd, baseURL, cleanup);
            }

            @Override
            protected void writeResultBody(StreamingWriter w, RevCommit result)
                    throws StreamWriterException {
                ResponseWriter out = new ResponseWriter(w, getMediaType());
                out.writeCommit(result, "commit", null, null, null);
            }

            @Override
            protected void writeError(StreamingWriter w, Throwable cause) throws StreamWriterException {
                if (cause instanceof MergeConflictsException) {
                    Context context = cmd.getContext();
                    MergeConflictsException m = (MergeConflictsException) cause;
                    final RevCommit ours = context.repository().getCommit(m.getOurs());
                    final RevCommit theirs = context.repository().getCommit(m.getTheirs());
                    final Optional<ObjectId> ancestor = context.command(FindCommonAncestor.class)
                            .setLeft(ours).setRight(theirs).call();
                    final MergeScenarioReport report = context.command(ReportMergeScenarioOp.class)
                            .setMergeIntoCommit(ours).setToMergeCommit(theirs).call();
                    ResponseWriter out = new ResponseWriter(w, getMediaType());
                    Optional<RevCommit> mergeCommit = Optional.absent();
                    w.writeStartElement("result");
                    out.writeMergeResponse(mergeCommit, report, ours.getId(),
                            theirs.getId(), ancestor.get());
                    w.writeEndElement();
                } else {
                    super.writeError(w, cause);
                }
            }
        }

    }


}