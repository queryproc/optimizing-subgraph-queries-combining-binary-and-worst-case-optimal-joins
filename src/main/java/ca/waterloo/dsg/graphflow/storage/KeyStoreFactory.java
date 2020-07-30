package ca.waterloo.dsg.graphflow.storage;

import ca.waterloo.dsg.graphflow.util.IOUtils;
import lombok.var;

import java.io.IOException;
import java.util.HashMap;

public class KeyStoreFactory {

    /**
     * Constructs a {@link KeyStore} object from binary serialized data.
     *
     * @param directoryPath is the directory to deserialize binary data.
     * @return the constructed graph object.
     * @throws IOException if stream to file cannot be written to or closed.
     * @throws ClassNotFoundException if the object read is from input stream is not found.
     */
    @SuppressWarnings("unchecked") // casting.
    public KeyStore make(String directoryPath) throws IOException, ClassNotFoundException {
        var stringToShortTypeKeyMap = (HashMap<String, Short>) IOUtils.deserializeObj(
            directoryPath + "TypesKeyMap");
        var stringToShortLabelKeyMap = (HashMap<String, Short>) IOUtils.deserializeObj(
            directoryPath + "LabelsKeyMap");
        var nextTypeKey = (short) IOUtils.deserializeObj(directoryPath + "nextTypeKey");
        var nextLabelKey = (short) IOUtils.deserializeObj(directoryPath + "nextLabelKey");
        return new KeyStore(stringToShortTypeKeyMap, stringToShortLabelKeyMap,
            nextTypeKey, nextLabelKey);
    }
}
