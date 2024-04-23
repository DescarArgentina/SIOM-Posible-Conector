﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Xml;

namespace SIOM_Conector
{
    class Xml_Intermedia
    {
        public void conversion(string connectionString, string archivoXML)
        {
            bool ban = true;
            XmlDocument xmlDoc = new XmlDocument();
            try
            {
                xmlDoc.Load($"{archivoXML}"); //Cargar el xml
            }
            catch (Exception ex)
            {
                Console.WriteLine("No se pudo cargar el archivo, pruebe otra vez. Error: " + ex.Message);
                return;
            }

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                XmlNode root = xmlDoc.DocumentElement;
                Dictionary<string, List<DataRow>> groupedDataRows = new Dictionary<string, List<DataRow>>();

                ParseNode(root, groupedDataRows); //Agrupar datos
                CreateTable(connection, groupedDataRows); //Creacion de tablas
                InsertData(connection, groupedDataRows); //Insercion de datos en las tablas

                if (ParseNode(root, groupedDataRows))
                {
                    if (ban)
                    {
                        BorrarTabla(connection, groupedDataRows);
                        ban = false;
                    }

                    CreateTable(connection, groupedDataRows);
                    InsertData(connection, groupedDataRows);
                }
            }
        }

        static bool ParseNode(XmlNode node, Dictionary<string, List<DataRow>> groupedDataRows, string parentNodeName = "")
        {
            var listaIgnorados = new List<string> { "ApplicationRef", "AssociatedDataSet", "AttributeContext", "DataSet",
                                            "ExternalFile", "Folder","ProductRevisionView", "RevisionRule",
                                            "Site", "Transform", "View" };

            try
            {
                if (node.NodeType == XmlNodeType.Element && !listaIgnorados.Contains(node.Name))
                {
                    string nodeName = node.Name; //Nombre actual del nodo
                    DataRow dataRow = new DataRow(); //Nuevo objeto datarow
                    dataRow.NombreNodo = nodeName;
                    dataRow.Atributos = new List<string>();

                    foreach (XmlAttribute attribute in node.Attributes)
                    {
                        dataRow.Atributos.Add(attribute.Name); //Guarda los nombres de los atributos
                    }
                    dataRow.XmlNode = node;
                    string tableName = GetTableName(nodeName, dataRow.Atributos, parentNodeName); //Creacion de nombre de la tabla

                    if (!groupedDataRows.ContainsKey(tableName))
                    {
                        groupedDataRows[tableName] = new List<DataRow>();
                    }
                    groupedDataRows[tableName].Add(dataRow);

                    foreach (XmlNode childNode in node.ChildNodes)
                    {
                        ParseNode(childNode, groupedDataRows, nodeName); //recursividad
                    }
                }
                return true;
            }
            catch (Exception ea)
            {
                //Utilidades.EscribirEnLog("Excepcion controlada en el metodo ParseNode: " + ea.Message);
                return false;
            }

        }

        static void CreateTable(SqlConnection connection, Dictionary<string, List<DataRow>> groupedDataRows)
        {
            foreach (var group in groupedDataRows)
            {
                string tableName = group.Key;
                if (tableName == "PLMXML") // Saltar el nodo "PLMXML"
                {
                    continue;
                }
                string createTableQuery = $"IF OBJECT_ID('[{tableName}]', 'U') IS NOT NULL DROP TABLE [{tableName}]; CREATE TABLE [{tableName}] (id INT IDENTITY(1,1) PRIMARY KEY, contenido NVARCHAR(MAX)";
                List<string> additionalAttributes = new List<string>();
                bool hasIdAttribute = false;

                foreach (DataRow dataRow in group.Value)
                {
                    foreach (string attribute in dataRow.Atributos)
                    {
                        if (!additionalAttributes.Contains(attribute) && attribute != "id") //Adicional atributo
                        {
                            additionalAttributes.Add(attribute);
                        }
                        if (attribute == "id") //Existe el atributo id
                        {
                            hasIdAttribute = true;
                        }
                    }
                }
                if (hasIdAttribute) // Existe el atributo "id", agregar id_Table
                {
                    createTableQuery += ", id_Table INT";
                }
                else // No existe el atributo "id", agregar id_Father
                {
                    createTableQuery += ", id_Father INT";
                }
                foreach (string columnName in additionalAttributes) //Creacion de columnas
                {
                    if (columnName != "id")
                    {
                        createTableQuery += $", [{columnName}] NVARCHAR(MAX)";
                    }
                }
                createTableQuery += ");";
                using (SqlCommand command = new SqlCommand(createTableQuery, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }

        static void InsertData(SqlConnection connection, Dictionary<string, List<DataRow>> groupedDataRows)
        {
            foreach (var group in groupedDataRows)
            {
                string tableName = group.Key;

                foreach (DataRow dataRow in group.Value)
                {
                    if (dataRow.NombreNodo == "PLMXML") // Saltar el nodo "PLMXML"
                        continue;
                    string insertQuery = $"INSERT INTO [{tableName}] (";
                    List<string> columnNames = new List<string>();
                    List<string> parameterNames = new List<string>();
                    List<SqlParameter> parameters = new List<SqlParameter>();
                    bool hasIdAttribute = false;

                    foreach (string columnName in dataRow.Atributos)
                    {

                        if (columnName == "id" || columnName == "instancedRef" || columnName == "masterRef" || columnName == "parentRef" || columnName == "instanceRefs") //Columna id_Table, instancedRef y masterRef
                        {
                            string attributeValue1 = dataRow.XmlNode.Attributes[columnName]?.Value;

                            if (columnName == "id" && !string.IsNullOrEmpty(attributeValue1) && attributeValue1.Length > 2)
                            {
                                hasIdAttribute = true;
                                columnNames.Add("[id_Table]");
                                parameterNames.Add("@id");
                                attributeValue1 = attributeValue1.Substring(2); //Suprimir los dos primeros caracteres
                                parameters.Add(new SqlParameter("@id", attributeValue1));
                            }
                            if (columnName == "instancedRef" && !string.IsNullOrEmpty(attributeValue1) && attributeValue1.Length > 2)
                            {
                                columnNames.Add("[instancedRef]");
                                parameterNames.Add("@instancedRef");
                                attributeValue1 = attributeValue1.Substring(3); //Suprimir los dos primeros caracteres
                                parameters.Add(new SqlParameter("@instancedRef", attributeValue1));
                            }
                            if (columnName == "masterRef" && !string.IsNullOrEmpty(attributeValue1) && attributeValue1.Length > 2)
                            {
                                columnNames.Add("[masterRef]");
                                parameterNames.Add("@masterRef");
                                attributeValue1 = attributeValue1.Substring(3); //Suprimir los dos primeros caracteres
                                parameters.Add(new SqlParameter("@masterRef", attributeValue1));
                            }
                            if (columnName == "parentRef" && !string.IsNullOrEmpty(attributeValue1) && attributeValue1.Length > 2)
                            {
                                columnNames.Add("[parentRef]");
                                parameterNames.Add("@parentRef");
                                attributeValue1 = attributeValue1.Substring(3); //Suprimir los tres primeros caracteres
                                parameters.Add(new SqlParameter("@parentRef", attributeValue1));
                            }
                            if (columnName == "instanceRefs" && !string.IsNullOrEmpty(attributeValue1) && attributeValue1.Length > 2)
                            {
                                columnNames.Add("[instanceRefs]");
                                parameterNames.Add("@instanceRefs");
                                attributeValue1 = attributeValue1.Substring(3); //Suprimir los tres primeros caracteres
                                parameters.Add(new SqlParameter("@instanceRefs", attributeValue1));
                            }
                            continue;
                        }
                        columnNames.Add($"[{columnName}]"); //Columnas de otros atributos que no son id,contenido y id_father
                        parameterNames.Add($"@{columnName}");
                        string attributeValue = dataRow.XmlNode.Attributes[columnName]?.Value;
                        attributeValue = attributeValue.Replace("'", "''");
                        parameters.Add(new SqlParameter($"@{columnName}", attributeValue));
                    }
                    columnNames.Add("[contenido]");//Columna contenido
                    parameterNames.Add("@contenido");
                    parameters.Add(new SqlParameter("@contenido", dataRow.XmlNode.InnerText));

                    if (!hasIdAttribute) //Columna de tablas sin id
                    {
                        columnNames.Add("[id_Father]");
                        parameterNames.Add("@idFather");
                        XmlNode parentNode = dataRow.XmlNode.ParentNode;
                        string parentAttributeValue = parentNode?.Attributes["id"]?.Value;
                        string parentAttributeId = parentAttributeValue?.Substring(2) ?? "0";
                        parameters.Add(new SqlParameter("@idFather", parentAttributeId));
                    }

                    insertQuery += string.Join(", ", columnNames) + ") VALUES (";
                    insertQuery += string.Join(", ", parameterNames) + ");";

                    using (SqlCommand command = new SqlCommand(insertQuery, connection))
                    {
                        command.Parameters.AddRange(parameters.ToArray());
                        command.ExecuteNonQuery();
                    }
                }
            }
        }

        static string GetTableName(string nodeName, List<string> attributes, string parentNodeName)
        {
            string tableName = nodeName;
            if (!attributes.Contains("id") && tableName != "PLMXML") //Si no tiene el atributo id y no es el nodo PLMXML
            {

                tableName = $"{nodeName}_{parentNodeName}";
            }
            return tableName;
        }

        static void BorrarTabla(SqlConnection connection, Dictionary<string, List<DataRow>> groupedDataRows)
        {
            foreach (var group in groupedDataRows)
            {
                try
                {
                    string tableName = group.Key;
                    string deleteTableQuery = $"IF OBJECT_ID('[{tableName}]', 'U') IS NOT NULL DROP TABLE [{tableName}]";
                    using (SqlCommand command = new SqlCommand(deleteTableQuery, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
                catch (Exception ea)
                {
                    //Utilidades.EscribirEnLog($"Error al intentar borrar la tabla para su sobreescritura - Error: {ea.Message}");
                }
            }
        }


    }
}
