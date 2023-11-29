package sqlloader;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.CommandLineRunner;
import org.springframework.beans.factory.annotation.Autowired;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

@SpringBootApplication
public class SqlLoaderApp 
  implements CommandLineRunner {

    private static Logger LOG = LoggerFactory.getLogger(SqlLoaderApp.class);

    @Autowired
    private SqlLoaderService loader;

    public static void main(String[] args) {
        LOG.info("STARTING THE APPLICATION");
        SpringApplication.run(SqlLoaderApp.class, args);
        LOG.info("APPLICATION FINISHED");
    }

    @Override
    public void run(String... args) {
        loader.load();
    }
}