package liquibase.ext.percona;

import java.util.Collections;
import java.util.List;

import liquibase.change.Change;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.core.AddPrimaryKeyChange;
import liquibase.change.core.DropPrimaryKeyChange;
import liquibase.database.Database;
import liquibase.statement.SqlStatement;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.util.StringUtils;
import liquibase.structure.core.Table;
import liquibase.structure.core.PrimaryKey;
import liquibase.structure.core.Schema;
import liquibase.snapshot.SnapshotGeneratorFactory;

/**
 * Subclasses the original {@link liquibase.change.core.AddPrimaryKeyChange} to
 * integrate with pt-online-schema-change.
 * @see PTOnlineSchemaChangeStatement
 */
@DatabaseChange(name = PerconaAddPrimaryKeyChange.NAME, description = "Adds a primary key to an existing table",
    priority = PerconaAddPrimaryKeyChange.PRIORITY, appliesTo = "column")
public class PerconaAddPrimaryKeyChange extends AddPrimaryKeyChange implements PerconaChange {
    public static final String NAME = "addPrimaryKey";
    public static final int PRIORITY = ChangeMetaData.PRIORITY_DEFAULT + 50;
    private static Logger log = LogFactory.getInstance().getLog();

    // Ensure we only set any AddPK options once.
    private static Boolean optsSet = false;


    private Boolean usePercona;

    /**
     * Generates the statements required for the add PK change change.
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
       
        // Add any specific safety options automatically to pt-osc for PK related changes (only once).
        if (optsSet == false) {

            StringBuilder extraProps = new StringBuilder();

            if (StringUtil.isNotEmpty(System.getProperty(Configuration.ADDITIONAL_OPTIONS))) { 
                extraProps.append(StringUtils.trimToEmpty(System.getProperty(Configuration.ADDITIONAL_OPTIONS)));
                extraProps.append(" --no-check-unique-key-change --no-check-alter");
            } else {
                extraProps.append("--no-check-unique-key-change --no-check-alter");
            }

            System.setProperty(Configuration.ADDITIONAL_OPTIONS, extraProps.toString());
            log.info("Added --no-check-unique-key-change --no-check-alter options to pt-osc");
            setOpts(true);
        }

        StringBuilder alter = new StringBuilder();

        // If there is an existing PK, there needs to be "drop_pk" added as a constraint.
        // This is needed because pt-osc won't allow a DROP without an ADD of PK.

        PrimaryKey example = new PrimaryKey();
        Table table = new Table();
        table.setSchema(new Schema(getCatalogName(), getSchemaName()));
        if (StringUtils.trimToNull(getTableName()) != null) {
            table.setName(getTableName());
        }
        example.setTable(table);
        example.setName("primary");

        try {
            if (!SnapshotGeneratorFactory.getInstance().has(example, database)) {
                log.info("Primary Key does not exist on " + database.escapeObjectName(getTableName(), Table.class));
            } else {
                log.info("Primary Key does exist on " + database.escapeObjectName(getTableName(), Table.class) + ", so append DROP PK");
                alter.append("DROP PRIMARY KEY, ");
            }
        } catch (Exception e) {
            boolean noPMD = true;
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
        // that's the percona drop primary key change
        DropPrimaryKeyChange inverse = new DropPrimaryKeyChange();
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

    public synchronized static void setOpts(boolean opts) {
        PerconaAddPrimaryKeyChange.optsSet = opts;
    }
}
