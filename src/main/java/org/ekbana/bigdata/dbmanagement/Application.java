package org.ekbana.bigdata.dbmanagement;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

import java.io.IOException;
import java.sql.Connection;

@SpringBootApplication
@EnableCaching
public class Application {

    static final Logger logger=Logger.getLogger(Application.class);
    static Connection postgresSQLDB_connection=null;


    Application() throws IOException {
        Application.restartConnection();
    }

    public static void restartConnection() throws IOException {
       postgresSQLDB_connection=new PostgresSQLDBConnection().get_or_create_connection();
    }



    public static void main(String[] args) {
        BasicConfigurator.configure();
        logger.info("application started");
        SpringApplication.run(Application.class,args);

    }
}
