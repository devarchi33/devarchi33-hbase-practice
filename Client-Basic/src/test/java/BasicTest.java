import client.GetTutorial;
import config.HBaseHelper;
import config.TutorialConfig;
import org.apache.hadoop.conf.Configuration;
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

    @Before
    public void setup() {
        AbstractApplicationContext ctx = new AnnotationConfigApplicationContext(TutorialConfig.class);
        conf = ctx.getBean("hBaseConfiguration", Configuration.class);
        helper = ctx.getBean("hBaseHelper", HBaseHelper.class);
        getTutorial = new GetTutorial(conf, helper);
    }

    @Test
    public void getTest() throws IOException {
        String val = getTutorial.get();
        assertEquals("val1", val);
    }
}
