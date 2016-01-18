package wrapper;

import org.apache.hadoop.hbase.client.Scan;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by donghoon on 2016. 1. 18..
 */
public class ScanForTutorial extends Scan implements Closeable {

    public ScanForTutorial() {
        super();
    }

    @Override
    public void close() throws IOException {
    }
}
