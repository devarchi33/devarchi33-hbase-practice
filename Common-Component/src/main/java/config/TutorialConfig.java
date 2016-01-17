package config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.ClassPathResource;

/**
 * Created by donghoon on 2016. 1. 17..
 */
@Configuration
public class TutorialConfig {

    @Value("${hbase.zookeeper.quorum}")
    private String hbaseHost;
    @Value("${hbase.zookeeper.property.clientPort}")
    private String hbaseClientPort;

    @Bean
    public static PropertySourcesPlaceholderConfigurer properties() {
        PropertySourcesPlaceholderConfigurer configurer = new PropertySourcesPlaceholderConfigurer();
        configurer.setLocation(new ClassPathResource("hbase.properties"));
        return configurer;
    }

    @Bean
    public org.apache.hadoop.conf.Configuration hBaseConfiguration() {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create(); //default $HBASE_HOME/conf/hbase-site.xml 정보 참조.
        conf.set("hbase.zookeeper.quorum", hbaseHost);
        conf.set("hbase.zookeeper.property.clientPort", hbaseClientPort);
        return conf;
    }
}
