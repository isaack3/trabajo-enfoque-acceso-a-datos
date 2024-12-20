/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package com.mycompany.trabajo_enfoque_acceso_a_datos;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Statement;
import java.util.Iterator;
import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.xml.sax.SAXException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

/**
 *
 * @author sebastiancamposrojas
 * 
 * Procesar el XML entregado desde la URL y importarlo en el proyecto: 
 * https://www.juntadeandalucia.es/datosabiertos/portal/dataset/91f3c95b-a2d2-4828-8880-99f8ea156d0e/resource/f7737217-65d6-4b00-bd10-2f82c69ae2f7/download/contratos-adjudicados-sep-24.xml
 */

public class Trabajo_enfoque_acceso_a_datos {
    
    /**
     * Variables constantes.
     */
    public static final String INPUT_FILE = "contratos-adjudicados-sep-24.xml";
    public static final String OUTPUT_FILE = "contratos-procesados.xml";
    public static final String DBNAME = "contratos";
    public static final String TBNAME = "adjudicados";
    
    /**
     * Método para declarar la variable tipo InputStream para la lectura del fichero XML
     * el cual contiene la información que se procesará.
     * @param file
     * @throws
     * @return
     */
    private static Document loadXMLDocument(String file){
        /**
         * Se declara variable de tipo Document 
         * para almacenar la información que será retornada desde el método.
         */
        Document document;
        
        /**
         * Se encapsula el proceso de lectura de fichero XML try/catch para evitar errores no controlados.
         */
        try {
            /**
             * Se declara variable de tipo InputStream, 
             * para seleccionar el fichero XML que se analizará,
             * en mi caso el fichero XML lo he almacenado en un directorio que he creado y lo he llamado resources/ 
             * y acceder aprovechando el metodo ofrecido por Java getResourceAsStream().
             */
            InputStream inputStream = Trabajo_enfoque_acceso_a_datos.class.getClassLoader().getResourceAsStream(file);
            if (inputStream == null) {
                System.err.println("Error: El archivo no encontrado en resources/.");
                return null;
            }
            
            /**
             * Se instancia la clase DocumentBuilderFactory para el procesado del fichero XML.
             */
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            
            /**
             * Se configura los espacios de nombre a verdadero, 
             * para que sea consciente que el fichero XML contiene espacios de nombres :ss, :x, :o entre otros.
             */
            factory.setNamespaceAware(true);
            
            /**
             * Se crea un objeto de DocumentBuilder para leer y analizar XML
             * y convertir en una estructura manipulable en memoria.
             */
            DocumentBuilder builder = factory.newDocumentBuilder();
            
            /**
             * se declara variable document para leer y convertir el fichero XML 
             * en un objeto manipulable (árbol de nodos).
             */
            document = builder.parse(inputStream);
            
        } catch (IOException | ParserConfigurationException | SAXException e) {
            document = null;
            System.err.println("Error: " + e.getMessage());
        }
        
        return document;
    }
    
    /**
     * Método para declarar la variable tipo OutputStream para la salida del fichero XML
     * el cual contendrá la información previamente procesada.
     * @param document
     * @param filename 
     * @throws
     */
    private static void saveXMLDocument(Document document, String filename) {
        /**
         * Se encapsula el proceso de lectura de fichero XML con try/catch para evitar errores no controlados.
         */
        try {
            /**
             * Se declara variable de tipo OutputStream 
             * para escribir en un fichero utilizando la clase FileOutputStream de acceso basados en bytes.
             */
            OutputStream outputStream = new FileOutputStream(new File(filename));
            
            /**
             * Se instancia la clase TransformerFactory para crear objetos
             * y realizar la transformación de ficheros XML.
             */
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            
            /**
             * Se crea un objeto de Transformer para transformar un fichero XML en otro formato.
             */
            Transformer transformer = transformerFactory.newTransformer();
            
            /**
             * Se configura la salida del fichero, y se permite que este tenga una indentación.
             */
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            
            /**
             * Se declara que el fichero XML de salida tiene el origen desde un documento DOM
             * y transformar a un flujo de salida.
             */
            transformer.transform(new DOMSource(document), new StreamResult(outputStream));
        } catch (FileNotFoundException | IllegalArgumentException | TransformerException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
    
    /**
     * Método para seleccionar la base de datos a utilizar.
     * @param dbname 
     * @param conn 
     * @param stmt 
     * @throws 
     */
    public static void selectDatabase(String dbname, Connection conn, Statement stmt){
        /**
         * Se encapsula el proceso de creación de bases de datos con try/catch para evitar errores no controlados.
         */
        try {
            String sql = "USE " + dbname;
            stmt.executeUpdate(sql);
            
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
    
    /**
     * Método para la creación de la bases de datos.
     * @param dbname 
     * @throws 
     */
    public static void createDatabase(String dbname){
        Connection conn = null;
        Statement stmt = null;
        
        /**
         * Se encapsula el proceso de creación de bases de datos con try/catch para evitar errores no controlados.
         */
        try {
            String sql = "CREATE DATABASE IF NOT EXISTS " + dbname;
            conn = getConnection();
            stmt = conn.createStatement();
            stmt.executeUpdate(sql);
            
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            
        } finally {
            
            /**
            * Se encapsula el proceso de cerrado de conexión de bases de datos con try/catch para evitar errores no controlados.
            */
            try {
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                System.err.println("Error al conexión de bases de datos: " + e.getMessage());
            }
        }
    }
    
    /**
     * Método para la creación de tabla donde irán almacenados los datos obtenidos del XML.
     * @param columns
     * @param dbname
     * @param tbname 
     * @throws
     */
    public static void createTableDatabase(String[] columns, String dbname, String tbname){
        Connection conn = null;
        Statement stmt = null;
        
        /**
         * Se encapsula el proceso de creación de bases de datos con try/catch para evitar errores no controlados.
         */
        try {
            
            /**
             * Se crea dinamicamente la tabla en base de datos.
             */
            String sql = "CREATE TABLE IF NOT EXISTS " + tbname + "(";
            for (int x = 0; x < columns.length; x++) {
                if (x == columns.length - 1) {
                    sql += "FECHA_DE_ADJUDICACION".equals(columns[x]) ? columns[x] + " DATETIME," : columns[x] + " VARCHAR(100)";
                } else {
                    sql += "FECHA_DE_ADJUDICACION".equals(columns[x]) ? columns[x] + " DATETIME," : columns[x] + " VARCHAR(100),";
                }
            }
            sql += ")";
            
            conn = getConnection();
            stmt = conn.createStatement();
            
            /**
             * Seleccionado de base de datos antes de ejecutar el create table.
             */
            selectDatabase(dbname, conn, stmt);
            
            /**
             * Ejecutado de la sentencia de creación de tabla.
             */
            stmt.executeUpdate(sql);
            
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            
            /**
             * Con finally me aseguro que pasará siempre por acá 
             * y que cerrará la conexión y la declaración de ejecución del SQL.
             */
        } finally {
            
            /**
            * Se encapsula el proceso de cerrado de conexión de bases de datos con try/catch para evitar errores no controlados.
            */
            try {
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                System.err.println("Error al conexión de bases de datos: " + e.getMessage());
            }
        }
    }
    
    /**
     * Método para el insertado de datos en la tabla de bases de datos previamente creada.
     * @param dbname
     * @param tbname
     * @param titles
     * @param data 
     * @throws 
     */
    public static void insertValuesDatabase(String dbname, String tbname, String[] titles, String[][] data) {
        Connection conn = null;
        Statement stmt = null;
        
        /**
         * Se encapsula el proceso de guardado de datos con try/catch para evitar errores no controlados.
         */
        try {
            String sql = "INSERT INTO " + tbname + "(";
            for (int x = 0; x < titles.length; x++) {
                if (x == titles.length - 1) {
                    sql += titles[x] + ") VALUES (";
                } else {
                    sql += titles[x] + ",";
                }
            }
            
            for (int x = 1; x < data.length; x++) {
                //System.out.println(data[x][0].trim());
                for (int y = 0; y < data[x].length; y++) {
                    if (x - 1 < data.length && y < data[x - 1].length) {
                        data[x - 1][y] = data[x][y];
                    }
                    if (x == data.length - 1) {
                        sql += "'" + data[x][y] + "',";
                    } else {
                        sql += "'" + data[x][y] + "'";
                    }
                }
            }
            sql += ")";
            
            // System.out.println(sql);
            
            conn = getConnection();
            stmt = conn.createStatement();
            stmt.executeUpdate(sql);
            System.out.println("Datos insertados.");
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
        } finally {
            try {
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                System.err.println("Error al conexión de bases de datos: " + e.getMessage());
            }
        }
    }

    /**
     * Metodo que crea la conexión con la base de datos mysql.
     * @return
     * @throws SQLException 
     */
    public static Connection getConnection() throws SQLException {
        String url = "jdbc:mysql://localhost:3306";
        String user = "root";
        String password = "";
        
        return DriverManager.getConnection(url, user, password);
    }
    
    /**
     * Método principal a ejecutar.
     * @param args 
     * @throws
     */
    public static void main(String[] args) {
        Document doc = loadXMLDocument(INPUT_FILE);
        
        /**
         * Si la variable de retorno doc es distinto a null, comenzará a leer los nodos del XML.
         */
        if (doc != null) {
            /**
             * Se instancia y configura xPath para moverse entre nodos.
             */
            XPath xPath = XPathFactory.newInstance().newXPath();
            
            /**
             * Se configura los espacios de nombre para el tratado de los elementos mediante xPath.
             */
            xPath.setNamespaceContext(new NamespaceContext() {
                @Override
                public String getNamespaceURI(String prefix) {
                    if (prefix.equals("ss")) {
                        return "urn:schemas-microsoft-com:office:spreadsheet";
                    }
                    return XMLConstants.NULL_NS_URI;
                }

                @Override
                public String getPrefix(String namespaceURI) {
                    if (namespaceURI.equals("urn:schemas-microsoft-com:office:spreadsheet")) {
                        return "ss";
                    }
                    return null;
                }

                @Override
                public Iterator<String> getPrefixes(String namespaceURI) {
                    return null;
                }
            });
            
            /**
            * Se encapsula el procesado de los datos XML con try/catch para evitar errores no controlados.
            */
            try {
                /**
                 * Se declara expresión de XPath para seleccionar y acceder a los elementos del Worksheet,
                 * se evalua la expresión aplicada en el Document 
                 * y configuramos que el retorno sera un NodeList con la variable constante NODESET.
                 */
                String expression = "//ss:Worksheet/ss:Table/ss:Row";
                NodeList nodeList = (NodeList) xPath.evaluate(expression, doc, XPathConstants.NODESET);

                /**
                 * Se declara configuración para el fichero de salida XML.
                 */
                Document outputDoc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
                Element workbookElement = outputDoc.createElement("Workbook");
                outputDoc.appendChild(workbookElement);
                
                /**
                 * Se crea elemento para contener los titulos.
                 */
                Element titlesRowElement = outputDoc.createElement("Titles");
                workbookElement.appendChild(titlesRowElement);
                
                /**
                 * Se declara elemento para seleccionar la primera fila y acceder a los elementos,
                 * se evalua la primera fila y configuramos que el retorno sera un NodeList con la variable constante NODESET.
                 */
                Element firstRow = (Element) nodeList.item(0);
                NodeList titleCells = (NodeList) xPath.evaluate("ss:Cell/ss:Data", firstRow, XPathConstants.NODESET);
                
                /**
                 * Se itera por cada elemento de los titulos del XML.
                 */
                String[] titles = new String[titleCells.getLength()];
                for (int i = 0; i < titleCells.getLength(); i++) {
                    String title = titleCells.item(i).getTextContent();
                    Element titleElement = outputDoc.createElement("Title");
                    Text titleText = outputDoc.createTextNode(title);
                    titleElement.appendChild(titleText);
                    titlesRowElement.appendChild(titleElement);
                    
                    /**
                     * Se almacena los datos en el array pero evitando tildes y 'ñ' para luego insertarlos en BD.
                     */
                    titles[i] = title.replace(" ", "_")
                    .replace("á", "a")
                    .replace("é", "e")
                    .replace("í", "i")
                    .replace("ó", "o")
                    .replace("ú", "u")
                    .replace("Á", "A")
                    .replace("É", "E")
                    .replace("Í", "I")
                    .replace("Ó", "O")
                    .replace("Ú", "U")
                    .replace("ñ", "n")
                    .replace("Ñ", "N");
                }
                
                /**
                 * Se crea elemento para contener las filas de datos.
                 */
                String[][] data = new String[nodeList.getLength() - 1][titleCells.getLength()];
                Element dataRowsElement = outputDoc.createElement("Rows");
                workbookElement.appendChild(dataRowsElement);
                for (int i = 1; i < nodeList.getLength(); i++) {
                    
                    /**
                    * Se declara elemento para seleccionar las filas de datos y acceder a los elementos,
                    * se evalua las filas de datos y configuramos que el retorno sera un NodeList con la variable constante NODESET.
                    */
                    Element rows = (Element) nodeList.item(i);
                    NodeList rowData = (NodeList) xPath.evaluate("ss:Cell/ss:Data", rows, XPathConstants.NODESET);
                    Element dataRowElement = outputDoc.createElement("Row");
                    
                    /**
                    * Se crea elemento para contener las filas de datos con su información.
                    */
                    for (int j = 0; j < rowData.getLength(); j++) {
                        String row = rowData.item(j).getTextContent();
                        Element dataElement = outputDoc.createElement("Data");
                        Text dataText = outputDoc.createTextNode(row);
                        dataElement.appendChild(dataText);
                        dataRowElement.appendChild(dataElement);
                        
                        /**
                        * Se almacena los datos en el array para luego insertarlos en BD.
                        */
                        if (i - 1 < data.length && j < data[i - 1].length) {
                            data[i - 1][j] = row;
                        }
                    }
                    dataRowsElement.appendChild(dataRowElement);
                }
                
                /**
                 * Se prepara base de datos, tabla e insertado de datos.
                 */
                createDatabase(DBNAME);
                createTableDatabase(titles, DBNAME, TBNAME);
                insertValuesDatabase(DBNAME, TBNAME, titles, data);

                /**
                 * Se guarda fichero XML de salida.
                 */
                saveXMLDocument(outputDoc, OUTPUT_FILE);
                System.out.println("El fichero XML de salida se ha generado correctamente en: " + OUTPUT_FILE);
            } catch (XPathExpressionException | ParserConfigurationException e) {
                System.err.println("Error: " + e.getMessage());
            }
        }
    }
}
