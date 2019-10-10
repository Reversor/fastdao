package io.github.pastorgl.fastdao;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface LOB {

    LobType type();

    enum LobType {
        BLOB,
        CLOB
    }
}