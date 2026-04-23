package com.saludvital.route;

import com.saludvital.processor.CsvValidatorProcessor;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FileTransferRoute extends RouteBuilder {

    @Autowired
    private CsvValidatorProcessor csvValidatorProcessor;

    @Override
    public void configure() throws Exception {

        onException(Exception.class)
            .log("ERROR INESPERADO - ${exception.message}")
            .to("file:data/error")
            .handled(true);

        from("file:data/input?delete=true&delay=3000&include=.*\\.csv&readLock=changed&readLockCheckInterval=1000")
            .routeId("saludvital-file-transfer")
            .log("INICIO - Archivo detectado: ${header.CamelFileName}")
            .process(csvValidatorProcessor)
            .choice()
                .when(header("csvValid").isEqualTo(true))
                    .log("VALIDO - ${header.CamelFileName}")
                    .to("file:data/output")
                    .setHeader("CamelFileName", header("archivedFileName"))
                    .to("file:data/archive")
                    .log("FIN VALIDO - ${header.CamelFileName}")
                .otherwise()
                    .log("INVALIDO - ${header.CamelFileName} - Error: ${header.validationError}")
                    .to("file:data/error")
                    .setHeader("CamelFileName", header("archivedFileName"))
                    .to("file:data/archive")
                    .log("FIN INVALIDO - ${header.CamelFileName}")
            .end();
    }
}
