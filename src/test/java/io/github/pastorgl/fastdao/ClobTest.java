package io.github.pastorgl.fastdao;

import io.github.pastorgl.fastdao.FastDAO.Transaction;
import io.github.pastorgl.fastdao.LOB.LobType;
import java.util.List;
import org.junit.Test;

import java.sql.SQLException;

public class ClobTest {

    @Test
    public void transactions() throws SQLException {
        TestDao dao = new TestDao();
        try (Transaction transaction = dao.getTransaction()) {

            TestEntity created = new TestEntity();
            transaction.insert(created);

            transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class TestDao extends FastDAO<TestEntity> {

        @Override
        protected TestTransaction getTransaction() throws SQLException {
            return new TestTransaction();
        }

        public class TestTransaction extends Transaction {

            TestTransaction() throws SQLException {
            }

            @Override
            protected void update(TestEntity entity) {
                super.update(entity);
            }

            @Override
            protected void update(List<TestEntity> enitities) {
                super.update(enitities);
            }

            @Override
            protected List<TestEntity> select(String query, Object... args) {
                return super.select(query, args);
            }

            @Override
            protected void insert(List<TestEntity> objects) {
                super.insert(objects);
            }

            @Override
            protected Object insert(TestEntity object) {
                return super.insert(object);
            }

            @Override
            protected void delete(List<TestEntity> objects) {
                super.delete(objects);
            }

            @Override
            protected void delete(TestEntity object) {
                super.delete(object);
            }

            @Override
            protected List<TestEntity> getAll() {
                return super.getAll();
            }

            @Override
            protected TestEntity getByPK(Object pk) {
                return super.getByPK(pk);
            }

            @Override
            protected void deleteByPK(Object pk) {
                super.deleteByPK(pk);
            }
        }
    }

    private static class TestEntity extends FastEntity {

        @PK
        private Integer id;
        @LOB(type = LobType.CLOB)
        private String text;
        @LOB(type = LobType.BLOB)
        private byte[] blob;


        @Override
        public Object getId() {
            return null;
        }
    }

}
