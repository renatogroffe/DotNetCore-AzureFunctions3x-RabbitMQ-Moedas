using System;
using System.Text.Json;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using ServerlessMoedas.Models;
using Dapper;

namespace ServerlessMoedas
{
    public static class MoedasRabbitMQTrigger
    {
        [FunctionName("MoedasRabbitMQTrigger")]
        public static void Run(
            [RabbitMQTrigger("queue-cotacoes", ConnectionStringSetting = "BrokerRabbitMQ")]string inputMessage,
            ILogger log)
        {
            var cotacao =
                JsonSerializer.Deserialize<Cotacao>(inputMessage);

            if (!String.IsNullOrWhiteSpace(cotacao.Sigla) &&
                cotacao.Valor.HasValue && cotacao.Valor > 0)
            {
                using (var conexao = new SqlConnection(
                    Environment.GetEnvironmentVariable("BaseCotacoes")))
                {

                    if (conexao.QueryFirst<int>(
                        "SELECT 1 FROM dbo.Cotacoes WHERE Sigla = @SiglaCotacao",
                        new { SiglaCotacao = cotacao.Sigla }) == 1)
                    {
                        conexao.Execute("UPDATE dbo.Cotacoes SET " +
                            "Valor = @ValorCotacao, " +
                            "UltimaCotacao = GETDATE() " +
                            "WHERE Sigla = @SiglaCotacao",
                            new
                            {
                                ValorCotacao = cotacao.Valor,
                                SiglaCotacao = cotacao.Sigla
                            });
                    }
                }

                log.LogInformation($"MoedasRabbitMQTrigger: {inputMessage}");
            }
            else
                log.LogError($"MoedasRabbitMQTrigger - Erro validação: {inputMessage}");
        }
    }
}