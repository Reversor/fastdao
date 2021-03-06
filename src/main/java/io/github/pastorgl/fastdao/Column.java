package io.github.pastorgl.fastdao;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Column {
    String value();

    Class<? extends StoreConverter> store() default StoreConverter.NullConverter.class;

    Class<? extends RetrieveConverter> retrieve() default RetrieveConverter.NullConverter.class;
}
