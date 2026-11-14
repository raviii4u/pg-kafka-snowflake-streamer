CREATE OR REPLACE PROCEDURE MERGE_CDC_BATCH_WITH_HISTORY(
    TARGET_TABLE STRING,
    TEMP_TABLE STRING,
    KEY_COLUMNS STRING,  -- Comma-separated primary key columns
    DATABASE_NAME STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    var VERSION = 'v5.5';
    try {
        // Get target table qualification using passed database name
        var targetParts = TARGET_TABLE.split('.');
        var qualifiedTarget = targetParts.length === 2 ? DATABASE_NAME + '.' + targetParts[0].toUpperCase() + '.' + targetParts[1].toUpperCase() : TARGET_TABLE;
        var keyColumns = KEY_COLUMNS.split(',').map(col => col.trim().toUpperCase());
        
        // Auto-discover columns from temp table
        var columnsResult = snowflake.execute({sqlText: `DESCRIBE TABLE ${TEMP_TABLE}`});
        var allColumns = [];
        while (columnsResult.next()) {
            allColumns.push(columnsResult.getColumnValue(1));
        }
        
        // MERGE into main table  
        var joinCondition = keyColumns.map(col => `t.${col} = s.${col}`).join(' AND ');
        var updateCols = allColumns.filter(col => !keyColumns.includes(col));
        var insertCols = allColumns.join(', ');
        var insertVals = allColumns.map(col => `s.${col}`).join(', ');
        var updateColsWithTimestamp = updateCols.map(col => `t.${col} = s.${col}`).join(', ');
        
        var mergeSQL = updateCols.length === 0 ? 
            `MERGE INTO ${qualifiedTarget} t USING ${DATABASE_NAME}.PUBLIC.${TEMP_TABLE} s ON ${joinCondition} WHEN NOT MATCHED AND s._IS_DELETED = FALSE THEN INSERT (${insertCols}) VALUES (${insertVals}) WHEN MATCHED AND s._IS_DELETED = TRUE THEN DELETE` :
            `MERGE INTO ${qualifiedTarget} t USING ${DATABASE_NAME}.PUBLIC.${TEMP_TABLE} s ON ${joinCondition} WHEN MATCHED AND s._IS_DELETED = FALSE THEN UPDATE SET ${updateColsWithTimestamp} WHEN NOT MATCHED AND s._IS_DELETED = FALSE THEN INSERT (${insertCols}) VALUES (${insertVals}) WHEN MATCHED AND s._IS_DELETED = TRUE THEN DELETE`;
        
        snowflake.execute({sqlText: mergeSQL});
        
        // Cleanup temp table
        try {
            snowflake.execute({sqlText: `DROP TABLE IF EXISTS ${DATABASE_NAME}.PUBLIC.${TEMP_TABLE}`});
        } catch (dropErr) {
            return `ERROR (${VERSION}): ${dropErr.message}`;
        }
        
        return 'SUCCESS (' + VERSION + '): Processed ' + allColumns.length + ' columns';
        
    } catch (err) {
        try { snowflake.execute({sqlText: `DROP TABLE IF EXISTS ${DATABASE_NAME}.PUBLIC.${TEMP_TABLE}`}); } catch (e) {}
        return `ERROR (${VERSION}): ${err.message}`;
    }
$$;

-- Grant usage to your CDC role:
-- GRANT USAGE ON PROCEDURE <YOUR_DATABASE>.PUBLIC.MERGE_CDC_BATCH_WITH_HISTORY(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE <YOUR_ROLE>;
