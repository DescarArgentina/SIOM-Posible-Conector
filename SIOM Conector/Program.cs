using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Xml;

namespace SIOM_Conector
{
    class Program
    {
        public static void Main(string[] args)
        {
            string connectionIntermedia = "Data Source=localhost;Initial Catalog=EBOM_SIOM;User ID=Renan;Password=Renan";
            string connectionIntercambio = "Data Source=localhost;Initial Catalog=EBOM2;User ID=Renan;Password=Renan";
            string archivoXML = "C:\\Users\\Deplm-07\\Desktop\\CONECTOR BASICO\\010003296_4_2-MDCA65_598x768_S20x60.xml";
            Xml_Intermedia xml_Intermedia = new Xml_Intermedia();
            Intermedia_Intercambio intermedia_Intercambio = new Intermedia_Intercambio();
            xml_Intermedia.conversion(connectionIntermedia,archivoXML);
            intermedia_Intercambio.traspaso(connectionIntermedia, connectionIntercambio);
        }
    }
}