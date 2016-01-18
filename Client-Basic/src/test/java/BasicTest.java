import client.GetTutorial;
import client.PutTutorial;
import client.ScanTutorial;
import config.HBaseHelper;
import config.TutorialConfig;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by donghoon on 2016. 1. 17..
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TutorialConfig.class})
public class BasicTest {

    private Configuration conf;
    private HBaseHelper helper;

    private GetTutorial getTutorial;
    private PutTutorial putTutorial;
    private ScanTutorial scanTutorial;

    @Before
    public void setup() {
        AbstractApplicationContext ctx = new AnnotationConfigApplicationContext(TutorialConfig.class);
        conf = ctx.getBean("hBaseConfiguration", Configuration.class);
        helper = ctx.getBean("hBaseHelper", HBaseHelper.class);
        getTutorial = new GetTutorial(conf, helper);
        putTutorial = new PutTutorial(conf, helper);
        scanTutorial = new ScanTutorial(conf, helper);
    }

    @Test
    public void getTest() throws IOException {
        String val = getTutorial.get("testtable", "row1", "colfam1", "qual1");
        assertEquals("value1", val);
    }

    @Test
    public void putTest() throws IOException {
        String[] qualifiers = {"qual1", "qual2"};
        String[] values = {"value1", "value2"};

        putTutorial.put("testtable", "row1", "colfam1", qualifiers, values);
        String val = getTutorial.get("testtable", "row1", "colfam1", "qual1");

        assertEquals("value1", val);
    }

    @Test
    public void scanTest() throws IOException {
        scanTutorial.scan();
    }

    @After
    public void tearDown() throws IOException {
        helper.close();
    }
}
