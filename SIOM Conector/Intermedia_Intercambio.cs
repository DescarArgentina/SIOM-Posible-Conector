using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SIOM_Conector
{
    class Intermedia_Intercambio
    {
        public void traspaso(string connectionIntermedia, string connectionIntercambio)
        {
            List<ProductDetail> products = new List<ProductDetail>();
            Dictionary<int, string> idToProductId = new Dictionary<int, string>();


            string query = @"WITH FirstQuery AS (SELECT DISTINCT
                                Occurrence.id_Table as id_Table,
                                --Occurrence.instancedRef,
                                ProductRevision.name,
                                Product.productId,
                                ProductRevision.revision,
                                COALESCE(UserValue_UserData.value, 'UNI') AS value,
                                CAST(Occurrence.parentRef as int) as parentRef
                                FROM
                                Occurrence
                                JOIN ProductRevision ON Occurrence.instancedRef = ProductRevision.id_Table
                                JOIN Product ON ProductRevision.masterRef = Product.id_Table
                                LEFT JOIN ProductInstance ON Occurrence.instanceRefs = ProductInstance.id_Table
                                LEFT JOIN Unit ON CAST(SUBSTRING(ProductInstance.unitRef, 3, LEN(ProductInstance.unitRef) - 2) AS INT) = Unit.id_Table
                                LEFT JOIN UserValue_UserData ON Unit.id_Table = UserValue_UserData.id_Father-1),

                                -- Segunda consulta
                                SecondQuery AS (
                                    SELECT DISTINCT
                                        CAST(COALESCE(NULLIF(UserValue_UserData.value, ''), '1') AS DECIMAL) as Quantity,
                                        Occurrence.id_Table as id_Table
                                    FROM
                                        Occurrence
                                    LEFT JOIN
                                        UserValue_UserData ON Occurrence.id_Table = UserValue_UserData.id_Father-1
                                    WHERE
                                        UserValue_UserData.title = 'Quantity'
                                )


                                -- Unir las dos consultas
                                SELECT DISTINCT * FROM FirstQuery
                                JOIN SecondQuery ON FirstQuery.id_Table = SecondQuery.id_Table
                                ORDER BY
                                FirstQuery.parentRef,
                                FirstQuery.id_Table ASC;";


            using (SqlConnection connection = new SqlConnection(connectionIntermedia))
            {
                SqlCommand command = new SqlCommand(query, connection);
                connection.Open();
                SqlDataReader reader = command.ExecuteReader();


                while (reader.Read())
                {
                    var idTable = reader.GetInt32(reader.GetOrdinal("id_Table"));
                    var productId = reader["productId"].ToString();
                     
                    if (!idToProductId.ContainsKey(idTable))
                    {
                        idToProductId[idTable] = productId;
                    }

                    products.Add(new ProductDetail
                    {
                        IdTable = idTable,
                        Name = reader["name"].ToString(),
                        ProductId = productId,
                        Revision = reader["revision"].ToString(),
                        Value = reader["value"].ToString(),
                        ParentRef = reader.IsDBNull(reader.GetOrdinal("parentRef")) ? 0 : reader.GetInt32(reader.GetOrdinal("parentRef")),
                        Quantity = reader.GetDecimal(reader.GetOrdinal("Quantity"))
                    });
                }
                reader.Close();
            }

            var summarizedProducts = products
                .GroupBy(p => new { p.Name, p.ProductId, p.ParentRef })
                .Select(g => new {
                    Name = g.Key.Name,
                    ProductId = g.Key.ProductId,
                    ParentRef = g.Key.ParentRef,
                    TotalQuantity = g.Sum(p => p.Quantity),
                    // Asumimos que los otros valores son consistentes dentro de cada grupo
                    Revision = g.First().Revision,
                    Value = g.First().Value,
                    IdTable = g.First().IdTable
                });

            using (SqlConnection connection = new SqlConnection(connectionIntercambio))
            {
                connection.Open();

                string createTableQuery = @"
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'MBOM')
                BEGIN
                    CREATE TABLE MBOM (
                        Name VARCHAR(255),
                        ProductId VARCHAR(255),
                        Revision VARCHAR(255),
                        UoM VARCHAR(255),
                        ParentRef INT,
                        Quantity DECIMAL(18, 2),
                        Response INT
                    );
                END
            ";

                SqlCommand createTableCommand = new SqlCommand(createTableQuery, connection);
                createTableCommand.ExecuteNonQuery();

                foreach (var product in summarizedProducts)
                {
                    int effectiveParentRef = idToProductId.ContainsKey(product.ParentRef) ? int.Parse(idToProductId[product.ParentRef]) : product.ParentRef;
                    string insertCommand = $"INSERT INTO MBOM (Name, ProductId, Revision, UoM, ParentRef, Quantity) VALUES (@Name, @ProductId, @Revision, @Value, @ParentRef, @TotalQuantity)";

                    using (SqlCommand cmd = new SqlCommand(insertCommand, connection))
                    {
                        cmd.Parameters.AddWithValue("@Name", product.Name);
                        cmd.Parameters.AddWithValue("@ProductId", product.ProductId);
                        cmd.Parameters.AddWithValue("@Revision", product.Revision);
                        cmd.Parameters.AddWithValue("@Value", product.Value);
                        cmd.Parameters.AddWithValue("@ParentRef", effectiveParentRef);
                        cmd.Parameters.AddWithValue("@TotalQuantity", product.TotalQuantity);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }
        }
    }
