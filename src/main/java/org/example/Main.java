package org.example;

import org.apache.commons.cli.*;
import org.example.ds.DataSource;
import org.example.utils.XmlUtils;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws ParseException, SQLException {
        System.out.println(Arrays.toString(args));
        final String ADD_COMMAND = "A";
        final String FIND_COMMAND = "F";
        final String CREATE_XML_FILE = "C";
        final String MAN_COMMAND = "man";
        if (org.example.utils.DbUtils.dbNotExists()) {
            //Создание таблиц
            org.example.utils.DbUtils.initDb();
            //Заполнение данными
            org.example.utils.DbUtils.findOrCreateWellAndAddEquipments(DataSource.getConnection(), "well1", 5);
            org.example.utils.DbUtils.findOrCreateWellAndAddEquipments(DataSource.getConnection(), "well2", 5);
            org.example.utils.DbUtils.findOrCreateWellAndAddEquipments(DataSource.getConnection(), "well3", 5);
        }

        Options options = new Options();
        Option addEquipmentToWellOpt = Option.builder()
                .longOpt(ADD_COMMAND)
                .argName("property=value")
                .hasArgs()
                .valueSeparator()
                .numberOfArgs(2)
                .desc("""
                        Добавить к скважине <wellName> оборудование 
                        в количестве <count>
                        -Awell=<wellName> -Acount=<count> 
                        example -Awell=well1 -Acount=10
                        """)
                .build();

        Option findWellByNameOpt = new Option(FIND_COMMAND, true,
                "Вывести оборудование по имени скважины");

        Option createFileOpt = new Option(CREATE_XML_FILE, true, "Генерация xml файла");
        Option manOpt = new Option(MAN_COMMAND, false, "Справка по командам");

        options.addOption(findWellByNameOpt);
        options.addOption(addEquipmentToWellOpt);
        options.addOption(createFileOpt);
        options.addOption(manOpt);

        HelpFormatter formatter = new HelpFormatter();

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption(ADD_COMMAND)) {
            Properties properties = cmd.getOptionProperties(ADD_COMMAND);
            String wellName =  properties.getProperty("well");
            int countEquipment = Integer.parseInt(properties.getProperty("count").trim());
            org.example.utils.DbUtils.findOrCreateWellAndAddEquipments(DataSource.getConnection(), wellName, countEquipment);
        } else if (cmd.hasOption(FIND_COMMAND)) {
            org.example.utils.DbUtils.showEquipmentsByNamesWell(org.example.utils.DbUtils.parseToNames(args));
        } else if (cmd.hasOption(CREATE_XML_FILE)) {
            XmlUtils.createOrderXmlFile(org.example.utils.DbUtils.getAllWellAndEquipments(), args[1]);
        } else if(cmd.hasOption(MAN_COMMAND)) {
            formatter.printHelp("CLI_helper", options);
        } else {
            System.out.printf("Ошибка ввода команды:%s повторите попытку", Arrays.toString(args));
        }
        System.exit(0);
    }
}
