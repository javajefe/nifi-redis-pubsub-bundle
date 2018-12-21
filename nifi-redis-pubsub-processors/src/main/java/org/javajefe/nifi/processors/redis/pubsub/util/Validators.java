package org.javajefe.nifi.processors.redis.pubsub.util;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * Created by Alexander Bukarev on 14.11.2018.
 */
public class Validators {

    /**
     * {@link Validator} that ensures that value is a  hostname:port
     */
    public static final Validator HOSTNAME_PORT_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            // expression language
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }
            // not empty
            ValidationResult nonEmptyValidatorResult = StandardValidators.NON_EMPTY_VALIDATOR.validate(subject, input, context);
            if (!nonEmptyValidatorResult.isValid()) {
                return nonEmptyValidatorResult;
            }
            // check format
            String hostnamePort = input;
            String[] addresses = hostnamePort.split(":");
            // Protect against invalid input like http://127.0.0.1:9300 (URL scheme should not be there)
            if (addresses.length != 2) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation(
                        "Must be in hostname:port form (no scheme such as http://").valid(false).build();
            }

            // Validate the port
            String port = addresses[1].trim();
            ValidationResult portValidatorResult = StandardValidators.PORT_VALIDATOR.validate(subject, port, context);
            if (!portValidatorResult.isValid()) {
                return portValidatorResult;
            }
            return new ValidationResult.Builder().subject(subject).input(input).explanation("Valid definition").valid(true).build();
        }
    };
}
