using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace SIOM_Conector
{
    class DataRow
    {
        public string NombreNodo { get; set; }
        public List<string> Atributos { get; set; }
        public XmlNode XmlNode { get; set; }
    }
}
