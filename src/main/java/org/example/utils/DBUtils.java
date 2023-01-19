package org.example.utils;

import org.example.domain.Equipment;
import org.example.domain.Well;
import org.example.ds.DataSource;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class DBUtils {

    private static final String CREATE_WELL_BY_NAME = "INSERT INTO well(NAME) VALUES(?);";
    private static final String SELECT_WELL_BY_NAME = """  
            SELECT * FROM well WHERE name = ?""";
    private static final String CREATE_EQUIPMENT_BY_WELL_ID = """
            INSERT INTO equipment(NAME, WELL_ID) VALUES(?,?);""";
    private static final String SEARCH_EQUIPMENT_BY_NAME_WELL = """
                    SELECT w.name, COUNT(e.id) FROM well w  \s
                        left join equipment e on w.ID = e.WELL_ID
                    WHERE w.name = ?; 
            """;
    private static final String SEARCH_EQUIPMENT_BY_NAMES_WELL = """
                    SELECT w.name, COUNT(e.id) FROM well w  \s
                        left join equipment e on w.ID = e.WELL_ID
                    WHERE w.name in 
            """;
    private static final String GROUP_BY_WELL_NAME = """
             GROUP BY w.NAME; 
            """;
    private static final String SEARCH_ALL_WELLS_AND_EQUIPMENTS = """
                   SELECT w.ID, w.NAME, e.ID, e.NAME, e.WELL_ID FROM well w 
                           LEFT JOIN equipment e on w.ID = e.WELL_ID
                   GROUP BY w.ID, w.NAME, e.ID, e.NAME, e.WELL_ID;     
            """;
    private static final String EXISTS_TABLE_WELL_AND_EQUPMENT = """
            SELECT name FROM sqlite_schema
            WHERE
                type ='table' AND
                name ='well' OR name ='equipment';
            """;
    private static final String CREATE_TABLE_WELL = """
            CREATE TABLE IF NOT EXISTS well(
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                NAME CHAR(32) NOT NULL UNIQUE
            );
            """;
    private static final String CREATE_TABLE_EQUPMENT = """
                     CREATE TABLE IF NOT EXISTS equipment(
                         ID INTEGER PRIMARY KEY AUTOINCREMENT,
                         NAME CHAR(32) NOT NULL UNIQUE,
                         WELL_ID INTEGER NOT NULL,
                         FOREIGN KEY(WELL_ID) REFERENCES well(ID)
                     );
            """;

    public static void createWell(String name) throws SQLException {
        final String SQL_QUERY = "INSERT INTO well(NAME) VALUES(?);";
        try (final Connection con = DataSource.getConnection();
             final PreparedStatement pStM = con.prepareStatement(SQL_QUERY, 1)) {
            final int i = pStM.executeUpdate();
            System.out.println(i);
        }
    }
  /** Метод добавляет оборудование к скважине, если скважины нет она создается
   * с именем указанным в wellName
   * @param wellName наименование искомой скважины
   * @param countEquipment количество оборудования добавленное к скважине
   */
    public static void findOrCreateWellAndAddEquipments(String wellName, Integer countEquipment) {
        if (countEquipment <= 0)
            throw new IllegalArgumentException("Количество скважин не может быть 0 или отрицательным числом");
        PreparedStatement pStM = null;
        ResultSet rs = null;
        try (final Connection con = DataSource.getConnection()) {
            pStM = con.prepareStatement(SELECT_WELL_BY_NAME);
            pStM.setString(1, wellName);
            rs = pStM.executeQuery();
            //когда не нашли скважину, создаем
            if (!rs.next()) {
                pStM = con.prepareStatement(CREATE_WELL_BY_NAME);
                pStM.setString(1, wellName);
                pStM.execute();
                pStM = con.prepareStatement(SELECT_WELL_BY_NAME);
                pStM.setString(1, wellName);
                rs = pStM.executeQuery();
                addEquimpent(countEquipment, con, rs);
            } else {
                addEquimpent(countEquipment, con, rs);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (rs != null) rs.close();
            } catch (SQLException se) {
                System.out.println(se.getMessage());
            }
            try {
                if (pStM != null) pStM.close();
            } catch (SQLException se) {
                System.out.println(se.getMessage());
            }
        }
    }

    private static void addEquimpent(Integer countEquipment, Connection con, ResultSet rs) throws SQLException {
        PreparedStatement pStM;
        int idWell = rs.getInt("id");
        String nameWell = rs.getString("name");
        for (int i = 1; i <= countEquipment; i++) {
            pStM = con.prepareStatement(CREATE_EQUIPMENT_BY_WELL_ID);
            pStM.setString(1, nameWell + "-" + UUID.randomUUID());
            pStM.setInt(2, idWell);
            pStM.execute();
        }
    }

    public static void showEquipmentsByNamesWell(Collection<String> nameWells) {
        String names = nameWells.stream().map(DBUtils::quote).collect(Collectors.joining(",", "(", ")"));
        try (final Connection con = DataSource.getConnection();
             final Statement st = con.createStatement();
             final ResultSet rs = st.executeQuery(SEARCH_EQUIPMENT_BY_NAMES_WELL + names + GROUP_BY_WELL_NAME)) {
            if (!rs.next()) {
                System.out.println("Не найдено такой/их скважин/ы:" + names.toString());
            } else {
                do {
                    String res = "НА скважине:" + rs.getString(1)
                            + " установлено  оборудование в количестве:"
                            + rs.getString(2) + " шт.";
                    System.out.println("--------------------------------");
                    System.out.println(res);
                } while (rs.next());
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static List<Well> getAllWellAndEquipments() {
        try (final Connection con = DataSource.getConnection();
             final Statement st = con.createStatement();
             final ResultSet rs = st.executeQuery(SEARCH_ALL_WELLS_AND_EQUIPMENTS)) {
            var wells = new HashSet<Well>();
            var fillingWells = new HashSet<Well>();
            var equipments = new ArrayList<Equipment>();
            while (rs.next()) {
                final Well well = new Well(rs.getLong(1),
                        rs.getString(2),
                        new ArrayList<>());
                wells.add(well);
                if (rs.getLong(3) != 0) {
                    final Equipment equipment = new Equipment(rs.getLong(3),
                            rs.getLong(5),
                            rs.getString(4));
                    equipments.add(equipment);
                }
            }
            for (Well curWell : wells) {
                for (Equipment equipment : equipments) {
                    if (Objects.equals(curWell.id(), equipment.wellId())) {
                        curWell.equipments().add(equipment);
                    }
                }
            }
            if (!wells.isEmpty()) {
                return wells.stream().sorted(Comparator.comparingLong(Well::id)).toList();
            }
            return Collections.emptyList();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return Collections.emptyList();
    }

    //Возможно не лучший вариант можно было бы использовать JSON.quote(), но не хочется тянуть лишнюю зависимость
    private static String quote(String n) {
        return "'" + n + "'";
    }

    public static void initDb() {

        PreparedStatement pStm = null;
        try (final Connection con = DataSource.getConnection();) {
            pStm = con.prepareStatement(CREATE_TABLE_WELL);
            pStm.execute();
            pStm = con.prepareStatement(CREATE_TABLE_EQUPMENT);
            pStm.execute();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        } finally {
            try {
                if (pStm != null) pStm.close();
            } catch (SQLException se) {
                System.out.println(se.getMessage());
            }
        }
    }
    public static boolean dbNotExists() {
        try (Connection con = DataSource.getConnection();
             PreparedStatement pStm = con.prepareStatement(EXISTS_TABLE_WELL_AND_EQUPMENT);
             ResultSet rs = pStm.executeQuery()) {
            while (rs.next()) {
                return rs.getLong(1) == 2;
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            return true;
        }
        return true;
    }

    public static Set<String> parseToNames(String[] args) {
        Set<String> wellNames = new HashSet<String>();
        // -F name1,name2,...
        if (args.length == 2 && args[1].contains(",")) {
            wellNames = Arrays.stream(args[1].split(",")).collect(Collectors.toSet());
        } else {
            for (int i = 1; i < args.length; i++) {
                String wellName = args[i];
                if (wellName.endsWith(",")) {
                    wellNames.add(wellName.substring(wellName.length() - 1));
                } else {
                    wellNames.add(wellName);
                }
            }
        }
        return wellNames;
    }
}
