package org.example.utils;

import org.example.domain.Equipment;
import org.example.domain.Well;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.util.List;

public class XmlUtils {
    public static void createOrderXmlFile(List<Well> wells, String fileName) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        try {
            builder = factory.newDocumentBuilder();

            Document doc = builder.newDocument();
            Element rootElement =
                    doc.createElement("dbinfo");
            doc.appendChild(rootElement);

            for (Well well : wells) {
                Node wellElement = getWellElement(doc, String.valueOf(well.id()), well.name());
                for (Equipment equipment : well.equipments()) {
                    getEquipmentElement(doc, wellElement, String.valueOf(equipment.id()), equipment.name(), String.valueOf(equipment.wellId()));
                }
                rootElement.appendChild(wellElement);
            }

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();

            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            DOMSource source = new DOMSource(doc);

            StreamResult console = new StreamResult(System.out);
            StreamResult file = new StreamResult(new File(fileName));

            transformer.transform(source, console);
            transformer.transform(source, file);
            System.out.println("Создание XML файла закончено");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Node getWellElement(Document doc, String id, String wellName) {
        Element well = doc.createElement("well");
        well.setAttribute("id", id);
        well.setAttribute("name", wellName);
        return well;
    }

    private static void getEquipmentElement(Document doc, Node wellElement, String id, String name, String wellId) {
        Element equipment = doc.createElement("equipment");
        equipment.setAttribute("id", id);
        equipment.setAttribute("well_id", wellId);
        equipment.setAttribute("name", name);
        wellElement.appendChild(equipment);
    }
}
