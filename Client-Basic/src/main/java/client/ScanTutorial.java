package client;

import config.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wrapper.ScanForTutorial;

import java.io.IOException;

/**
 * Created by donghoon on 2016. 1. 17..
 */
public class ScanTutorial {
    private Logger logger = LoggerFactory.getLogger(ScanTutorial.class);
    private Configuration conf;
    private HBaseHelper helper;

    public ScanTutorial(Configuration conf, HBaseHelper helper) {
        this.conf = conf;
        this.helper = helper;
    }

    public void scan() throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf("testtable"));
             ScanForTutorial scan1 = new ScanForTutorial();
             ScanForTutorial scan2 = new ScanForTutorial();
             ScanForTutorial scan3 = new ScanForTutorial();
             ScanForTutorial scan4 = new ScanForTutorial();
             ScanForTutorial scan5 = new ScanForTutorial()) {

            helper.dropTable("testtable");
            helper.createTable("testtable", "colfam1", "colfam2");
            logger.info("Adding rows to table...");
            // Tip: Remove comment below to enable padding, adjust start and stop
            // row, as well as columns below to match. See scan #5 comments.
            helper.fillTable("testtable", 1, 100, 100, /* 3, false, */ "colfam1", "colfam2");

            logger.info("Scanning table #1...");
            // vv ScanExample

            ResultScanner scanner1 = table.getScanner(scan1); // co ScanExample-2-GetScanner Get a scanner to iterate over the rows.
            for (Result res : scanner1) {
                logger.info("Result: " + res); // co ScanExample-3-Dump Print row content.
            }

            logger.info("Scanning table #2...");
            scan2.addFamily(Bytes.toBytes("colfam1")); // co ScanExample-5-AddColFam Add one column family only, this will suppress the retrieval of "colfam2".
            ResultScanner scanner2 = table.getScanner(scan2);
            for (Result res : scanner2) {
                logger.info("Result: " + res);
            }

            logger.info("Scanning table #3...");
            scan3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5")).
                    addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("col-33")). // co ScanExample-6-Build Use fluent pattern to add specific details to the Scan.
                    setStartRow(Bytes.toBytes("row-10")).
                    setStopRow(Bytes.toBytes("row-20"));
            ResultScanner scanner3 = table.getScanner(scan3);
            for (Result res : scanner3) {
                logger.info("Result: " + res);
            }

            logger.info("Scanning table #4...");
            scan4.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5")). // co ScanExample-7-Build Only select one column.
                    setStartRow(Bytes.toBytes("row-10")).
                    setStopRow(Bytes.toBytes("row-20"));
            ResultScanner scanner4 = table.getScanner(scan4);
            for (Result res : scanner4) {
                logger.info("Result: " + res);
            }

            logger.info("Scanning table #5...");
            scan5.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5")).
                    setStartRow(Bytes.toBytes("row-20")).
                    setStopRow(Bytes.toBytes("row-10")).
                    setReversed(true); // co ScanExample-8-Build One column scan that runs in reverse.
            ResultScanner scanner5 = table.getScanner(scan5);
            for (Result res : scanner5) {
                logger.info("Result: " + res);
            }

        } finally {
            helper.close();
        }
    }
}
