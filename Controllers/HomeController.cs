using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using Microsoft.AspNetCore.Mvc;


namespace EtlTool.Controllers
{
    public class HomeController : Controller
    {
        public string sqlconStr = "data source=***;Database=testDb;User Id=***;password=***8;Trusted_Connection=False;MultipleActiveResultSets=true;";
        public string oracleconStr = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=****)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=****)));User Id=***;Password=****;";
        public IActionResult Index()
        {
            return View();
        }

        public async Task<ActionResult> SqlConnectionManager()
        {
            try
            {                 
                SqlConnectionManager sqlcon = new SqlConnectionManager(new SqlConnectionString(sqlconStr));
                                  
                ControlFlow.DefaultDbConnection = sqlcon;

                CreateTableTask.Create(sqlcon, "ATABLE1", new List<TableColumn>()
                {                     
                    new TableColumn("VCH_DIST_NAME","varchar(40)",allowNulls:true),
                    new TableColumn("INT_DIST_ID","varchar(40)",allowNulls:true)
                });
                 

                CsvSource<string[]> source = new CsvSource<string[]>("t_covid_care_center.csv");                 
                RowTransformation<string[], MyData> row = new RowTransformation<string[], MyData>
                (
                     input => new MyData() { VCH_DIST_NAME = input[1], INT_DIST_ID = input[2] }                     
                );                                 

                
               DbDestination<MyData> destSql  = new DbDestination<MyData>(sqlcon, "ATABLE1");

                source.LinkTo(row);
                row.LinkTo(destSql);
                await source.ExecuteAsync();
                destSql.Wait(); 

                return View();
            }
            catch (Exception ex)
            {
                 throw ex;
            }
        }

        public async Task<ActionResult> OracleConnectionManager()
        {
            try
            {
                OracleConnectionManager orcon = new OracleConnectionManager(new OracleConnectionString(oracleconStr));                 

                ControlFlow.DefaultDbConnection = orcon;                 

                CreateTableTask.Create(orcon, "ATABLE1", new List<TableColumn>()
                {
                    new TableColumn("VCH_DIST_NAME","varchar2(20)",allowNulls:true),
                    new TableColumn("INT_DIST_ID","varchar2(20)",allowNulls:true)
                });


                CsvSource<string[]> source = new CsvSource<string[]>("t_covid_care_center.csv");
                RowTransformation<string[], MyData> row = new RowTransformation<string[], MyData>
                (
                     input => new MyData() { VCH_DIST_NAME = input[1], INT_DIST_ID = input[2] }
                );


                DbDestination<MyData> destOralce = new DbDestination<MyData>(orcon, "ATABLE1");   

                source.LinkTo(row);
                row.LinkTo(destOralce);
                await source.ExecuteAsync();
                destOralce.Wait();

                return View();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //[ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        //public IActionResult Error()
        //{
        //    return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        //}
    }

     
    public class MyData
    {
        //public int INT_ID { get; set; }
        public string INT_DIST_ID { get; set; }
        public string VCH_DIST_NAME { get; set; }
    }
     
}
