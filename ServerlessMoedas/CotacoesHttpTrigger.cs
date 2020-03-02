using System;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Dapper;
using ServerlessMoedas.Models;

namespace ServerlessMoedas
{
    public static class CotacoesHttpTrigger
    {
        [FunctionName("CotacoesHttpTrigger")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
            ILogger log)
        {
            string moeda = req.Query["moeda"];
            log.LogInformation($"CotacoesHttpTrigger: {moeda}");

            if (!String.IsNullOrWhiteSpace(moeda))
            {
                using var conexao = new SqlConnection(
                    Environment.GetEnvironmentVariable("BaseCotacoes"));

                return (ActionResult)new OkObjectResult(
                    await conexao.QueryFirstOrDefaultAsync<Cotacao>(
                        "SELECT * FROM dbo.Cotacoes " +
                        "WHERE Sigla = @SiglaMoeda",
                        new { SiglaMoeda = moeda }
                    )
                );
            }
            else
            {
                return new BadRequestObjectResult(new
                {
                    Sucesso = false,
                    Mensagem = "Informe uma sigla de moeda válida"
                });
            }
        }
    }
}