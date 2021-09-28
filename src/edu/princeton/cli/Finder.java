package edu.princeton.cli;

import org.dspace.content.DSpaceObject;
import org.dspace.content.MetadataField;
import org.dspace.core.Context;
import org.dspace.storage.rdbms.DatabaseManager;
import org.dspace.storage.rdbms.TableRow;
import org.dspace.storage.rdbms.TableRowIterator;

import java.sql.SQLException;
import java.util.ArrayList;

public class Finder {
    public static ArrayList<DSpaceObject> byMetadata(Context context, MetadataField mdField, String value, int type) throws SQLException {
        String sql = "SELECT MV.resource_id, MV.resource_type_id  FROM MetadataValue MV";
        sql = sql + String.format(" where MV.metadata_field_id= '%s'", mdField.getFieldID());
        if (null != value) {
            sql = sql + String.format(" AND MV.text_value LIKE '%s'", value);
        }
        if (type >= 0) {
            sql = sql + String.format(" AND MV.resource_type_id = '%d'", type);
        }

        TableRowIterator tri = DatabaseManager.queryTable(context, "MetadataValue", sql);
        ArrayList<DSpaceObject> dsos = new ArrayList<DSpaceObject>();
        TableRow iter;
        while (tri.hasNext()) {
            iter = tri.next();
            dsos.add(DSpaceObject.find(context, iter.getIntColumn("resource_type_id"), iter.getIntColumn("resource_id")));
        }
        tri.close();
        return dsos;
    }
}
