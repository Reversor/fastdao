package io.github.pastorgl.fastdao;

import io.github.pastorgl.fastdao.FastDAO.Transaction;
import org.junit.Test;

import java.sql.SQLException;

public class ClobTest {

    @Test
    public void transactions() throws SQLException {
        TestDao dao = new TestDao();
        try (Transaction transaction = dao.createTransaction()) {
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class TestDao extends FastDAO<TestEntity> {

    }

    private static class TestEntity extends FastEntity {

        @PK
        private Integer id;
        @CLOB
        private String text;
        @BLOB
        private byte[] blob;


        @Override
        public Object getId() {
            return null;
        }
    }

}