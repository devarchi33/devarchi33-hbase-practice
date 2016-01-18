package client;

import config.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by donghoon on 2016. 1. 17..
 */
public class GetTutorial {
    private Logger logger = LoggerFactory.getLogger(GetTutorial.class);
    private Configuration conf;
    private HBaseHelper helper;

    public GetTutorial(Configuration conf, HBaseHelper helper) {
        this.conf = conf;
        this.helper = helper;
    }

    public String get(String tableName, String row, String colfam,
                      String qualifier) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf("testtable"))) {//최상단 테이블 객체.

            if (!helper.existsTable(tableName)) {
                helper.createTable(tableName, colfam);
            }

            Get get = new Get(Bytes.toBytes(row)); //테이블 객체를 얻은 후 row 정보를 얻어옵니다.

            get.addColumn(Bytes.toBytes(colfam), Bytes.toBytes(qualifier)); //얻어올 컬럼 정보를 추가합니다.

            Result result = table.get(get); //테이블 객체를 통해서 get 인스턴스에 등록된 row의 column 정보를 읽어 들입니다.

            byte[] val = result.getValue(Bytes.toBytes(colfam),
                    Bytes.toBytes(qualifier));  //qulifier를 통해 특정 컬럼 값을 얻어옵니다.

            logger.info("Value: " + Bytes.toString(val));

            return Bytes.toString(val);
        }
    }
}
