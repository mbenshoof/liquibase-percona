package liquibase.ext.percona;

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import liquibase.change.AddColumnConfig;
import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.core.MySQLDatabase;
import liquibase.executor.ExecutorService;
import liquibase.executor.jvm.JdbcExecutor;
import liquibase.statement.SqlStatement;

public class PerconaCreateIndexChangeTest
{

    private PerconaCreateIndexChange c;
    private Database database;

    @Before
    public void setup() {
        c = new PerconaCreateIndexChange();
        AddColumnConfig column = new AddColumnConfig();
        column.setName( "indexedColumn" );
        c.addColumn( column );
        c.setTableName( "person" );
        c.setIndexName( "theIndexName" );
        c.setUnique( true );

        DatabaseConnectionUtil.passwordForTests = "root";

        database = new MySQLDatabase();
        database.setLiquibaseSchemaName("testdb");
        DatabaseConnection conn = new MockDatabaseConnection("jdbc:mysql://user@localhost:3306/testdb",
                "user@localhost");
        database.setConnection(conn);
        ExecutorService.getInstance().setExecutor(database, new JdbcExecutor());

        PTOnlineSchemaChangeStatement.available = true;
        System.setProperty(Configuration.FAIL_IF_NO_PT, "false");
        System.setProperty(Configuration.NO_ALTER_SQL_DRY_MODE, "false");
        
    }

    @Test
    public void testCreateNewIndexReal() {
        SqlStatement[] statements = c.generateStatements(database);
        Assert.assertEquals(1, statements.length);
        Assert.assertEquals(PTOnlineSchemaChangeStatement.class, statements[0].getClass());
        Assert.assertEquals("pt-online-schema-change --alter=\"CREATE UNIQUE INDEX theIndexName ON person(indexedColumn)\" "
                + "--alter-foreign-keys-method=auto "
                + "--host=localhost --port=3306 --user=user --password=*** --execute D=testdb,t=person",
                ((PTOnlineSchemaChangeStatement)statements[0]).printCommand(database));
    }
}
