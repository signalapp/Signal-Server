package org.whispersystems.textsecuregcm.util;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({ FIELD, METHOD, PARAMETER, ANNOTATION_TYPE })
@Retention(RUNTIME)
@Constraint(validatedBy = ExactlySizeValidator.class)
@Documented
public @interface ExactlySize {

  String message() default "{org.whispersystems.textsecuregcm.util.ExactlySize." +
      "message}";

  Class<?>[] groups() default { };

  Class<? extends Payload>[] payload() default { };

  int[] value();

  @Target({ FIELD, METHOD, PARAMETER, ANNOTATION_TYPE })
  @Retention(RUNTIME)
  @Documented
  @interface List {
    ExactlySize[] value();
  }
}
