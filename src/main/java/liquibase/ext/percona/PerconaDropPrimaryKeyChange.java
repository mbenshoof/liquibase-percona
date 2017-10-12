package liquibase.ext.percona;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.core.DropPrimaryKeyChange;
import liquibase.database.Database;
import liquibase.statement.SqlStatement;

/**
 * Subclasses the original {@link liquibase.change.core.DropPrimaryKeyChange} to
 * integrate with pt-online-schema-change.
 * @see PTOnlineSchemaChangeStatement
 */
@DatabaseChange(name = PerconaDropPrimaryKeyChange.NAME, description = "Drops an existing primary key",
    priority = PerconaDropPrimaryKeyChange.PRIORITY, appliesTo = "primaryKey")
public class PerconaDropPrimaryKeyChange extends DropPrimaryKeyChange implements PerconaChange {
    public static final String NAME = "dropPrimaryKey";
    public static final int PRIORITY = ChangeMetaData.PRIORITY_DEFAULT + 50;

    private Boolean usePercona;

    /**
     * Generates the statements required for the drop PK change.
     * This class is a placeholder and usePercona is hardcoded to 
     * be blocked since pt-osc doesn't support DROP PK.
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

        alter.append("DROP PRIMARY KEY");

        return alter.toString();
    }

    @Override
    public Boolean getUsePercona() {
        return usePercona;
    }

    public void setUsePercona(Boolean usePercona) {
        this.usePercona = false;
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
