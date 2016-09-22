package org.locationtech.geogig.geotools.zip;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.geotools.data.DataStore;
import org.locationtech.geogig.api.ProgressListener;
import org.locationtech.geogig.geotools.plumbing.DataStoreExportOp;

public class ZipDataStoreExportOp extends DataStoreExportOp<File> {

    private File shape;

    public ZipDataStoreExportOp setShapeFile(File shape) {
        this.shape = shape;
        return this;
    }


    @Override
    protected void export(final String refSpec, final DataStore targetStore,
            final String targetTableName, final ProgressListener progress) {

        super.export(refSpec, targetStore, targetTableName, progress);


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
