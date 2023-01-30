package org.example.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.example.domain.Equipment;
import org.example.domain.Well;
import org.example.ds.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DbUtils {

    private static final String INSERT_INTO_WELL_NAME = "INSERT INTO well(NAME) VALUES(?);";

    private DbUtils() {
        throw new UnsupportedOperationException("Нет необходимости "
                 + "создавать объект утилитного класса");
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DbUtils.class);

    private static final String CREATE_WELL_BY_NAME =
            "INSERT INTO well(NAME) VALUES(?);";
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
        try (Connection con = DataSource.getConnection();
             PreparedStatement pStM = con.prepareStatement(INSERT_INTO_WELL_NAME, 1)) {
            final int i = pStM.executeUpdate();
            System.out.println(i);
        }
    }
    /** Метод добавляет оборудование к скважине, если скважины нет она создается
     * с именем указанным в wellName
     * @param wellName наименование искомой скважины
     * @param countEquipment количество оборудования добавленное к скважине
     * @param con connection to database
     */

    public static void findOrCreateWellAndAddEquipments(Connection con, final String wellName, final Integer countEquipment) {
        final long start = System.nanoTime();
        LOGGER.info("findOrCreateWellAndEquipments wellName {} countEquipment {}",
                wellName, countEquipment);
        Objects.requireNonNull(wellName, () -> "Наименование скважины не может быть null");
        Objects.requireNonNull(countEquipment, () -> "Количество скважин не может быть null");
        Objects.requireNonNull(con, () -> "Соединение с БД не инициализировано");
        if (countEquipment <= 0)
            throw new IllegalArgumentException("Количество скважин не может быть 0 или отрицательным числом");
        PreparedStatement pStM = null;
        ResultSet rs = null;
        try {
            con.setAutoCommit(false);
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
            con.commit();
            System.out.println("Время выполнения метода findOrCreateWellAndAddEquipments:"
                    + TimeUnit.SECONDS.convert((System.nanoTime() - start), TimeUnit.NANOSECONDS));
        } catch (SQLException e) {
            try {
                con.rollback();
            } catch (SQLException ex) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
        } finally {
            try {
                if (rs != null) rs.close();
            } catch (SQLException se) {
                System.out.println(se.getMessage());
                se.printStackTrace();
            }
            try {
                if (pStM != null) pStM.close();
            } catch (SQLException se) {
                System.out.println(se.getMessage());
                se.printStackTrace();
            }
            try {
                con.close();
            } catch (SQLException se) {
                System.out.println(se.getMessage());
                se.printStackTrace();
            }
        }
    }

    private static void addEquimpent(final Integer countEquipment, final Connection con,final ResultSet rs) throws SQLException {
        final long start = System.nanoTime();
        int step = 10_000;
        System.out.println("step:" + step);
        PreparedStatement pStM = null;
        int idWell = rs.getInt("id");
        String nameWell = rs.getString("name");
        pStM = con.prepareStatement(CREATE_EQUIPMENT_BY_WELL_ID);
        for (int i = 1; i <= countEquipment; i++) {
            pStM.setString(1, nameWell + "-" + UUID.randomUUID());
            pStM.setInt(2, idWell);
            pStM.addBatch();
            if (i % step == 0) {
//          System.out.printf("iteration:%d, осталось:%d \n", i, countEquipment - i);
                pStM.executeBatch();
            }
        }
        pStM.executeBatch();
        System.out.printf("addEquipments time:%d countEquipment: %d \n",
                TimeUnit.SECONDS.convert((System.nanoTime() - start), TimeUnit.NANOSECONDS),
                countEquipment);
    }

    public static void showEquipmentsByNamesWell(final Collection<String> nameWells) {
        String names = nameWells.stream().map(DbUtils::quote).collect(Collectors.joining(",", "(", ")"));
        try (Connection con = DataSource.getConnection();
             Statement st = con.createStatement();
             ResultSet rs = st.executeQuery(SEARCH_EQUIPMENT_BY_NAMES_WELL + names + GROUP_BY_WELL_NAME)) {
            if (!rs.next()) {
                System.out.println("Не найдено такой/их скважин/ы:" + names);
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
        try (Connection con = DataSource.getConnection();
             Statement st = con.createStatement();
             ResultSet rs = st.executeQuery(SEARCH_ALL_WELLS_AND_EQUIPMENTS)) {
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
    private static String quote(final String n) {
        return "'" + n + "'";
    }

    public static void initDb() {

        PreparedStatement pStm = null;
        try (Connection con = DataSource.getConnection()) {
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

    public static Set<String> parseToNames(final String[] args) {
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
