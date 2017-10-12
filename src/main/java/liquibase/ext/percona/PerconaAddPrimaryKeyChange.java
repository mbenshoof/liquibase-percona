package liquibase.ext.percona;

import java.util.Collections;
import java.util.List;

import liquibase.change.Change;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.core.AddPrimaryKeyChange;
import liquibase.database.Database;
import liquibase.statement.SqlStatement;
import liquibase.util.StringUtils;

/**
 * Subclasses the original {@link liquibase.change.core.AddUniqueConstraintChange} to
 * integrate with pt-online-schema-change.
 * @see PTOnlineSchemaChangeStatement
 */
@DatabaseChange(name = PerconaAddPrimaryKeyChange.NAME, description = "Adds a primary key to an existing table",
    priority = PerconaAddPrimaryKeyChange.PRIORITY, appliesTo = "column")
public class PerconaAddPrimaryKeyChange extends AddPrimaryKeyChange implements PerconaChange {
    public static final String NAME = "addPrimaryKey";
    public static final int PRIORITY = ChangeMetaData.PRIORITY_DEFAULT + 50;

    private Boolean usePercona;

    /**
     * Generates the statements required for the add unique constraint change.
     * In case of a MySQL database, percona toolkit will be used.
     * In case of generating the SQL statements for review (updateSQL) the command
     * will be added as a comment.
     * @param database the database
     * @return the list of statements
     * @see PTOnlineSchemaChangeStatement
     */
    @Override
    public SqlStatement[] generateStatements(Database database) {
        return PerconaChangeUtil.generateStatements(this,
                database,
                super.generateStatements(database));
    }

    @Override
    public String generateAlterStatement(Database database) {
        StringBuilder alter = new StringBuilder();

        if (StringUtil.isNotEmpty(getConstraintName())) {
            if (StringUtils.trimToEmpty(getConstraintName()).equals("drop_pk")) {
                alter.append("DROP PRIMARY KEY, ");
            } 
        }

        alter.append("ADD PRIMARY KEY (");
        List<String> columns = StringUtils.splitAndTrim(getColumnNames(), ",");
        if (columns == null) columns = Collections.emptyList();
        alter.append(database.escapeColumnNameList(StringUtils.join(columns, ", ")));
        alter.append(")");

        return alter.toString();
    }

    @Override
    protected Change[] createInverses() {
        // that's the percona drop unique constraint change
        PerconaDropPrimaryKeyChange inverse = new PerconaDropPrimaryKeyChange();
        inverse.setSchemaName(getSchemaName());
        inverse.setTableName(getTableName());

        return new Change[] { inverse };
    }

    @Override
    public Boolean getUsePercona() {
        return usePercona;
    }

    public void setUsePercona(Boolean usePercona) {
        this.usePercona = usePercona;
    }

    @Override
    public String getChangeSkipName() {
        return NAME;
    }

    @Override
    public String getTargetTableName() {
        return getTableName();
    }
}
