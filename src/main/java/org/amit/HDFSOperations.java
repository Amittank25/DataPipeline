package org.amit;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.*;


/**
 * Author amittank.
 *
 * Create, Read, Delete operations on HDFS
 */
public class HDFSOperations {

    /**
     * Read a file from HDFS
     * @param file
     * @param configuration
     * @throws IOException
     */
    public void readFile(String file, Configuration configuration) throws IOException {
        org.apache.hadoop.fs.FileSystem fileSystem = org.apache.hadoop.fs.FileSystem.get(configuration);

        //Get the path of the file and check if the file exists.
        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exists");
            return;
        }

        //Create input and output stream and write bytes.
        FSDataInputStream in = fileSystem.open(path);
        String filename = file.substring(file.lastIndexOf('/') + 1, file.length());
        OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(filename)));
        byte[] b = new byte[1024];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        //Write data to console and close file descriptors.
        System.out.print(out.toString());
        in.close();
        out.close();
        fileSystem.close();
    }

    /**
     * Create directory
     * @param directory
     * @throws IOException
     */
    public void mkdir(String directory, Configuration configuration) throws IOException {
        org.apache.hadoop.fs.FileSystem fileSystem = org.apache.hadoop.fs.FileSystem.get(configuration);

        //Get the path of the file and check if file exists.
        Path path = new Path(directory);
        if (fileSystem.exists(path)) {
            System.out.println("Dir " + directory + " already not exists");
            return;
        }

        //Create a directory and close file descriptor.
        fileSystem.mkdirs(path);
        fileSystem.close();
    }

    /**
     * Add file to HDFS
     * @param sourceLocation
     * @param destinationLocation
     * @param cofiguration
     * @throws IOException
     */
    public void addFile(String sourceLocation, String destinationLocation, Configuration cofiguration) throws IOException {

        org.apache.hadoop.fs.FileSystem fileSystem = org.apache.hadoop.fs.FileSystem.get(cofiguration);

        //Extract file name from the file path
        String filename = sourceLocation.substring(sourceLocation.lastIndexOf('/') + 1,sourceLocation.length());

        //Create destination path
        if (destinationLocation.charAt(destinationLocation.length() - 1) != '/') {
            destinationLocation = destinationLocation + "/" + filename;
        } else {
            destinationLocation = destinationLocation + filename;
        }

        //Check if file already exists.
        Path path = new Path(destinationLocation);
        if (fileSystem.exists(path)) {
            System.out.println("File " + destinationLocation + " already exists");
            return;
        }

        //Create new file at the destination location and write data to it.
        FSDataOutputStream out = fileSystem.create(path);
        InputStream in = new BufferedInputStream(new FileInputStream(new File(
                sourceLocation)));

        byte[] b = new byte[1024];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        //Close the file descriptors
        in.close();
        out.close();
        fileSystem.close();
    }



    /**
     * Delete a file or Directory
     * @param file
     * @throws IOException
     */
    public void deleteFile(String file, Configuration configuration) throws IOException {
        org.apache.hadoop.fs.FileSystem fileSystem = org.apache.hadoop.fs.FileSystem.get(configuration);

        //Get the path of the file and check if file exists.
        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exists");
            return;
        }

        //Delete file or directory.
        fileSystem.delete(new Path(file), true);
        fileSystem.close();
    }
}

