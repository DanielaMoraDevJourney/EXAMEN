package com.saludvital.processor;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@Component
public class CsvValidatorProcessor implements Processor {

    private static final Logger log = LoggerFactory.getLogger(CsvValidatorProcessor.class);

    private static final String EXPECTED_HEADER = "patient_id,full_name,appointment_date,insurance_code";
    private static final Set<String> VALID_INSURANCE_CODES = new HashSet<>(Arrays.asList("IESS", "PRIVADO", "NINGUNO"));
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss");

    @Override
    public void process(Exchange exchange) throws Exception {
        String body = exchange.getIn().getBody(String.class);
        String fileName = exchange.getIn().getHeader("CamelFileName", String.class);

        String baseName = fileName.endsWith(".csv") ? fileName.substring(0, fileName.length() - 4) : fileName;
        String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMATTER);
        exchange.getIn().setHeader("archivedFileName", baseName + "_" + timestamp + ".csv");

        if (body == null || body.trim().isEmpty()) {
            markInvalid(exchange, "Archivo vacio", fileName);
            return;
        }

        String[] lines = body.split("\\r?\\n");

        String header = lines[0].trim();
        if (!EXPECTED_HEADER.equals(header)) {
            markInvalid(exchange, "Encabezado incorrecto: " + header, fileName);
            return;
        }

        for (int i = 1; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.isEmpty()) continue;

            String[] fields = line.split(",", -1);

            if (fields.length != 4) {
                markInvalid(exchange, "Linea " + i + ": se esperaban 4 campos, se encontraron " + fields.length, fileName);
                return;
            }

            for (String field : fields) {
                if (field.trim().isEmpty()) {
                    markInvalid(exchange, "Linea " + i + ": campo vacio detectado", fileName);
                    return;
                }
            }

            try {
                LocalDate.parse(fields[2].trim(), DATE_FORMATTER);
            } catch (DateTimeParseException e) {
                markInvalid(exchange, "Linea " + i + ": fecha invalida '" + fields[2].trim() + "' (esperado YYYY-MM-DD)", fileName);
                return;
            }

            if (!VALID_INSURANCE_CODES.contains(fields[3].trim())) {
                markInvalid(exchange, "Linea " + i + ": codigo de seguro invalido '" + fields[3].trim() + "'", fileName);
                return;
            }
        }

        exchange.getIn().setHeader("csvValid", true);
        exchange.getIn().setHeader("validationError", "");
        log.info("VALIDO - {}", fileName);
    }

    private void markInvalid(Exchange exchange, String error, String fileName) {
        exchange.getIn().setHeader("csvValid", false);
        exchange.getIn().setHeader("validationError", error);
        log.warn("INVALIDO - {} - Error: {}", fileName, error);
    }
}
