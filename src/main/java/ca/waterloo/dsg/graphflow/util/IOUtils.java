package ca.waterloo.dsg.graphflow.util;

import lombok.var;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Utils for Input, Output (I/O) and time operations.
 */
public class IOUtils {

    /**
     * Calculates the time difference between now and the given {@code beginTime}.
     *
     * @param beginTime The being time in nanoseconds.
     * @return Time difference in milliseconds.
     */
    public static double getElapsedTimeInMillis(long beginTime) {
        return (System.nanoTime() - beginTime) / 1000000.0;
    }

    /**
     *
     *
     * @param filename
     * @param output
     * @throws IOException
     */
    public static void log(String filename, String output) throws IOException {
        IOUtils.mkdirForFile(filename);
        IOUtils.createNewFile(filename);
        var writer = new FileWriter(filename, true /* append to the file */);
        writer.write(output);
        writer.flush();
    }

    /**
     * @see File#mkdir()
     */
    public static void mkdirs(String directoryPath) throws IOException {
        var file = new File(directoryPath);
        if (!file.exists()) {
            if (!file.mkdirs()) {
                throw new IOException("The directory " + directoryPath + " was not created.");
            }
        }
    }

    /**
     * @see File#createNewFile()
     */
    public static void createNewFile(String filePath) throws IOException {
        var file = new File(filePath);
        if (!file.exists()) {
            if (!file.createNewFile()) {
                throw new IOException("The file " + filePath + " was not created.");
            }
        }
    }

    /**
     * Creates an {@link ObjectOutputStream} object from the given {@code outputPath}.
     *
     * @param outputPath The {@link String} path to the output file.
     * @return an {@link ObjectOutputStream} object.
     */
    public static ObjectOutputStream makeObjectOutputStream(String outputPath)
        throws IOException {
        return new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(outputPath)));
    }

    /**
     * Creates an {@link ObjectInputStream} object from the given {@code inputFilePath}.
     *
     * @param inputFilePath The {@link String} path to the input file.
     * @return an {@link ObjectInputStream} object.
     */
    public static ObjectInputStream makeObjInputStream(String inputFilePath) throws IOException {
        return new ObjectInputStream(new BufferedInputStream(new FileInputStream(inputFilePath)));
    }

    public static void mkdirForFile(String outputFilename) throws IOException {
        String[] output_split = outputFilename.split("/");
        StringBuilder outputDirBuilder = new StringBuilder();
        for (int i = 0; i < output_split.length - 1; i++) {
            outputDirBuilder.append(output_split[i]);
            outputDirBuilder.append("/");
        }
        IOUtils.mkdirs(outputDirBuilder.toString());
    }

    /**
     * @param file
     * @param object
     * @throws IOException if stream to file cannot be written to or closed.
     */
    public static void serializeObj(String file, Object object) throws IOException {
        var outputStream = IOUtils.makeObjectOutputStream(file);
        outputStream.writeObject(object);
        outputStream.close();
    }

    /**
     * @param directory
     * @param filenameObjectPairs
     * @throws IOException if stream to file cannot be written to or closed.
     */
    public static void serializeObjs(String directory, Object[] filenameObjectPairs)
        throws IOException {
        for (int i = 0; i < filenameObjectPairs.length; i += 2) {
            serializeObj(directory + filenameObjectPairs[i], filenameObjectPairs[i + 1]);
        }
    }

    /**
     * @param file
     * @return
     * @throws IOException if stream to file cannot be written to or closed.
     * @throws ClassNotFoundException if the object read from input stream is not found.
     */
    public static Object deserializeObj(String file) throws IOException,
        ClassNotFoundException {
        var inputStream = IOUtils.makeObjInputStream(file);
        Object object = inputStream.readObject();
        inputStream.close();
        return object;
    }
}
