using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SIOM_Conector
{
    public class ProductDetail
    {
        public int IdTable { get; set; }
        public string Name { get; set; }
        public string ProductId { get; set; }
        public string Revision { get; set; }
        public string Value { get; set; }
        public int ParentRef { get; set; }
        public decimal Quantity { get; set; }
    }
}
