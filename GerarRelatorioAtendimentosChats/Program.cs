using CsvHelper;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;

namespace GerarRelatorioAtendimentosChats
{
    public class RelatorioNumeroChatsBean
    {
        public string Operacao { get; set; }
        public string Mes { get; set; }
        public int QuantidadeAtivo { get; set; }
        public int QuantidadeReceptivo { get; set; }
        //public string Telefone { get; set; }
    }

    public class RelatorioQuantidadeMensagem
    {
        public string Operacao { get; set; }
        public string Mes { get; set; }
        public int QuantidadeRecebidas { get; set; }
        public int QuantidadeEnviada { get; set; }
        public int QuantidadeSistema { get; set; }

    }
    public class RelatorioUsuariosFaturaveis
    {
        public string Cliente { get; set; }
        public string Login { get; set; }
        public string Nome { get; set; }
        public string Status { get; set; }
        public string Faturavel { get; set; }
    }
    public class RelatorioUsuariosFaturaveisVolt
    {
        public string Cliente { get; set; }
        public string Nome { get; set; }
        public string Email { get; set; }
        public string Status { get; set; }
        public string Deletado { get; set; }
    }
    public class RelatorioQuantidadeMensagensChatApi
    {
        public string Cliente { get; set; }
        public string Data { get; set; }
        public string Numero { get; set; }
        public string Recebidas { get; set; }
        public string Enviadas { get; set; }
        public string HSM { get; set; }
    }
    public class DadosBaseProdutos
    {
        public string Nome { get; set; }
        public string StringConexao { get; set; }
        public string NumeroTelefone { get; set; }
    }
    public class DadosBaseVolt
    {
        public string Nome { get; set; }
        public string StringConexao { get; set; }
    }
    class Program
    {
        private static readonly NLog.Logger _logger = NLog.LogManager.GetCurrentClassLogger();
        static void Main(string[] args)
        {
            while (true)
            {
                Console.WriteLine(" ");
                Console.WriteLine("-------------------INICIANDO SOLICITAÇÃO-------------------------");
                _logger.Info("-------------------INICIANDO SOLICITAÇÃO-------------------------");
                gerarRelatorio();

                Console.WriteLine(" ");
                Console.WriteLine("-------------------ENCERRANDO SOLICITAÇÃO-------------------------");
                _logger.Info("-------------------ENCERRANDO SOLICITAÇÃO-------------------------");
            }
        }

        private static void gerarRelatorio()
        {
            List<DadosBaseProdutos> listaDeStringsSql = new List<DadosBaseProdutos>();
            List<DadosBaseVolt> listaDeStringsSqlVolt = new List<DadosBaseVolt>();
            List<RelatorioNumeroChatsBean> listaCompletaNumeroChats = new List<RelatorioNumeroChatsBean>();
            List<RelatorioQuantidadeMensagem> listaCompletaQuantidadeMensages = new List<RelatorioQuantidadeMensagem>();
            List<RelatorioUsuariosFaturaveis> listaCompletaUsuariosFaturaveis = new List<RelatorioUsuariosFaturaveis>();
            List<RelatorioUsuariosFaturaveisVolt> listaCompletaUsuariosFaturaveisVolt = new List<RelatorioUsuariosFaturaveisVolt>();
            List<RelatorioQuantidadeMensagensChatApi> listaCompletaQuantidadeMensagensChatApi= new List<RelatorioQuantidadeMensagensChatApi>();

            #region connection strings Volt
            listaDeStringsSqlVolt.Add(new DadosBaseVolt { Nome = "Argo", StringConexao = "Server=10.0.0.4,1433; Database=ArgoVolt; User ID=c1app_argo; Password=4&*BMb?9WHg%72n$; Trusted_Connection=False;" });
            listaDeStringsSqlVolt.Add(new DadosBaseVolt { Nome = "Cartos", StringConexao = "Server=10.0.0.4,1433; Database=CartosVolt; User ID=c1app_cartos; Password=JbP:m!bbNx6atTjv; Trusted_Connection=False;" });
            listaDeStringsSqlVolt.Add(new DadosBaseVolt { Nome = "MeetaWeb", StringConexao = "Server=10.0.0.4,1433; Database=MeetaWebVolt; User ID=c1app_meetaweb; Password=+X9J{NB+M<$ewyhp; Trusted_Connection=False;" });
            #endregion

            #region connection strings produção

            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Amafresp", StringConexao = "Server=10.0.0.4,1433;Database=Amafresp;User ID=c1app_amafresp;Password=%@}>c`5bT[)xk+69;Trusted_Connection=False;", NumeroTelefone = "551129205166" });
            //listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Azul", StringConexao = "Server=10.0.0.4,1433;Database=AzulLinhasAereas;User ID=c1app_azullinhasaereas;Password=Gt^3VHY_Z$pRub_2;Trusted_Connection=False;", NumeroTelefone = "551132406881" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "ContactOne", StringConexao = "Server=10.0.0.4,1433;Database=ContactOne;User ID=c1app_contactone;Password=G4%nMWuQadu9ytMQ;Trusted_Connection=False;", NumeroTelefone = "551129205169" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Editora Moderna", StringConexao = "Server=10.0.0.4,1433;Database=EditoraModerna;User ID=c1app_editoramoderna;Password=L@jTr-x!d&Mg^d5M;Trusted_Connection=False;", NumeroTelefone = "551129205163" });
            //listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Espaço Laser", StringConexao = "Server=10.0.0.4,1433;Database=EspacoLaser;User ID=c1app_espacolaser;Password=G!q_9bwhJgoPtyqx;Trusted_Connection=False;", NumeroTelefone = "551129205160" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Ipiranga Adm", StringConexao = "Server=10.0.0.4,1433;Database=IpirangaAdministrativo;User ID=c1app_ipiranga;Password=wCm&zLV!*x9SCDM7;Trusted_Connection=False;", NumeroTelefone = "551132406892" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Ipiranga Combustivel", StringConexao = "Server=10.0.0.4,1433;Database=IpirangaCombustivel;User ID=c1app_ipiranga;Password=wCm&zLV!*x9SCDM7;Trusted_Connection=False;", NumeroTelefone = "551132406891" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Ipiranga Empresarial", StringConexao = "Server=10.0.0.4,1433;Database=IpirangaEmpresarial;User ID=c1app_ipiranga;Password=wCm&zLV!*x9SCDM7;Trusted_Connection=False;", NumeroTelefone = "551132406887" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Ipiranga Lubrificante", StringConexao = "Server=10.0.0.4,1433;Database=IpirangaLubrificante;User ID=c1app_ipiranga;Password=wCm&zLV!*x9SCDM7;Trusted_Connection=False;", NumeroTelefone = "551132406886" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Ipiranga VIP", StringConexao = "Server=10.0.0.4,1433;Database=IpirangaVIP;User ID=c1app_ipiranga;Password=wCm&zLV!*x9SCDM7;Trusted_Connection=False;", NumeroTelefone = "5511997215446" });
            //listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Kimberly Clark Sac", StringConexao = "Server=10.0.0.4,1433;Database=KimberlyClarkSac;User ID=c1app_kimberlyclark;Password=I4MSwimyV7egHv$r;Trusted_Connection=False;", NumeroTelefone = "5511973483095" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "LeBiscuit", StringConexao = "Server=10.0.0.4,1433;Database=LeBiscuit;User ID=c1app_lebiscuit;Password=K2!a?JVZNaLF=@!A;Trusted_Connection=False;", NumeroTelefone = "WEB" });
            //listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Meeta Solutions", StringConexao = "Server=10.0.0.4,1433;Database=MeetaWeb;User ID=c1app_meetaweb;Password=+X9J{NB+M<$ewyhp;Trusted_Connection=False;", NumeroTelefone = "5511956370047" });
            //listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Metlife Central Vendas", StringConexao = "Server=10.0.0.4,1433;Database=MetlifeCentralVendas;User ID=c1app_metlife;Password=sE1W3xXnsRz6wJ&b;Trusted_Connection=False;", NumeroTelefone = "551129205168" });
            //listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Prevent Senior", StringConexao = "Server=10.0.0.4,1433;Database=c1_chat;User ID=c1app_chat;Password=w3uefNsqrL;Trusted_Connection=False;", NumeroTelefone = "5511945595132" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "ProFrotas Central", StringConexao = "Server=10.0.0.4,1433;Database=ProFrotas;User ID=c1app_profrotas;Password=Bu3xBH7P9%UAPe5!;Trusted_Connection=False;", NumeroTelefone = "5511950647153" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "ProFrotas Backoffice", StringConexao = "Server=10.0.0.4,1433;Database=ProFrotasBackoffice;User ID=c1app_farmercentral;Password=6cF9Ag9Fj!^wC)&+;Trusted_Connection=False;", NumeroTelefone = "5511963745076" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Sercom Comercial", StringConexao = "Server=10.0.0.4,1433;Database=SercomComercial;User ID=c1app_sercomcomercial;Password=Mtnz&S@VhL@s5HR@;Trusted_Connection=False;", NumeroTelefone = "551132406880" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Sanofi", StringConexao = "Server=10.0.0.4,1433;Database=Sanofi;User ID=c1app_sanofi;Password=ylr^hG5B&2RjWEJ2;Trusted_Connection=False;", NumeroTelefone = "WEB" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Soluções Moderna", StringConexao = "Server=10.0.0.4,1433;Database=EdModernaAprovaBrasil;User ID=c1app_editoramodernaaprovabrasil;Password=ceYXZYWj9Za=C@Ww;Trusted_Connection=False;", NumeroTelefone = "551132406885" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Intercement", StringConexao = "Server=10.0.0.4,1433;Database=Intercement;User ID=c1app_intercement;Password=ZcT=[uyPmpJQ4hQS;Trusted_Connection=False;", NumeroTelefone = "5511943217378" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Allergan", StringConexao = "Server=10.0.0.4,1433;Database=Allergan;User ID=c1app_allergan;Password=WysyFv9@uSRnH~)Q;Trusted_Connection=False;", NumeroTelefone = "5511971551463" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Grupo Simões", StringConexao = "Server=10.0.0.4,1433;Database=GrupoSimoes;User ID=c1app_gruposimoes;Password=An&rZX{7agR!(N`-;Trusted_Connection=False;", NumeroTelefone = "web_gruposimoes" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Cardif", StringConexao = "Server=10.0.0.4,1433;Database=Cardif;User ID=c1app_cardif;Password=d/n78VzAe8[CQs]z;Trusted_Connection=False;", NumeroTelefone = "5511941997073" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Suzano", StringConexao = "Server=10.0.0.4,1433;Database=Suzano;User ID=c1app_suzano;Password=9nBYu53f.EdS)BPh;Trusted_Connection=False;", NumeroTelefone = "5511942982141" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Femsa", StringConexao = "Server=10.0.0.4,1433;Database=Femsa; User ID=c1app_femsa; Password=F26u4%&Ut2yKw+s@; Trusted_Connection=False;", NumeroTelefone = "5511973459142" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "EdModerna PNLD", StringConexao = "Server=10.0.0.4,1433;Database=EdModernaPNLD;User ID=c1app_editoramodernapnld;Password=Zh6NjaCQvY(CTm^+;Trusted_Connection=False;", NumeroTelefone = "551129205162" });
            listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "Caixa Antifraude", StringConexao = "Server=10.0.0.4,1433;Database=CaixaAntifraude;User ID=c1app_caixaantifraude;Password=+65Cbe3&N4uPyAkZ;Trusted_Connection=False;", NumeroTelefone = "5511955891624" });

            //listaDeStringsSql.Add(new DadosBaseProdutos { Nome = "", StringConexao = "", NumeroTelefone = "" });

            #endregion

            Console.WriteLine("Lista de bases de dados carregada com sucesso", Console.ForegroundColor);

            Console.WriteLine("Digite: \n1 - Relação de chats.\n2 - Relação de usuários faturáveis.\n3 - Gerar quantidade de mensagens\n4 - Usuários VOLT\n5 - Quantidade de mensagens ChatApi");
            var numberDigitado = Console.ReadLine();

            bool numeroFoiDigitado = int.TryParse(numberDigitado, out int numeroResposta);

            if (numeroFoiDigitado)
            {
                //DateTime dataRelatorio = new DateTime(DateTime.Now.Year, DateTime.Now.Month - 1, 1);
                DateTime dateMesPassado = DateTime.Now.AddMonths(-1);
                DateTime dataRelatorio = new DateTime(dateMesPassado.Year, dateMesPassado.Month, 1);

                if (numeroResposta == 0)
                {
                    Console.WriteLine("Digite os números informados");
                    Console.WriteLine(" ");
                    return;
                }

                if (numeroResposta == 1)
                {
                    #region relação chats
                    try
                    {
                        foreach (var dadosSQL in listaDeStringsSql)
                        {
                            Console.WriteLine($"Obtendo dados de: {dadosSQL.Nome}");
                            using (var connection = new SqlConnection(dadosSQL.StringConexao))
                            {
                                using (var command = new SqlCommand())
                                {
                                    connection.Open();

                                    command.Connection = connection;
                                    command.CommandType = System.Data.CommandType.Text;
                                    //command.CommandText = @$"
                                    //                    select [Operacao] = @base,	[Mes] = FORMAT(C._createdAt, 'MM/yyyy'), [Quantidade] = COUNT(C._id)
                                    //                    from chats C
                                    //                    where C._primary = 1 and C._createdAt BETWEEN '2020-04-01 00:00:00' and '2020-04-21 23:59:59'
                                    //                    group by FORMAT(C._createdAt, 'MM/yyyy')";
                                    command.CommandText = @$"
                                                select [Operacao] = @base,	[Mes] = FORMAT(C._createdAt, 'MM/yyyy'), 
                                                SUM(CASE WHEN C._agentSelectionId <> 5 THEN 1 ELSE 0 END) AS [QuantidadeReceptivo],
                                                SUM(CASE WHEN C._agentSelectionId = 5 THEN 1 ELSE 0 END) AS [QuantidadeAtivo]
                                                from chats C
                                                where C._primary = 1 and C._createdAt > '{dataRelatorio.ToString("yyyy-MM-dd")}'
                                                group by FORMAT(C._createdAt, 'MM/yyyy')";

                                    command.CommandTimeout = 0;

                                    command.Parameters.Add("@base", System.Data.SqlDbType.VarChar, 255).Value = dadosSQL.Nome;

                                    using (var reader = command.ExecuteReader())
                                    {
                                        while (reader.Read())
                                        {
                                            string mes = reader["Mes"].ToString();

                                            //if (string.IsNullOrWhiteSpace(mes))
                                            //{
                                            //    string operacao = reader["Operacao"].ToString();
                                            //    int quantidade = int.Parse(reader["Quantidade"].ToString());

                                            //    RelatorioBean bean = new RelatorioBean
                                            //    {
                                            //        Mes = dataRelatorio.ToString("MM/yyyy"),
                                            //        Operacao = sqlconnStringsDado.Key,
                                            //        Quantidade = 0
                                            //    };

                                            //    continue;
                                            //}

                                            DateTime mesData = DateTime.Parse(mes);

                                            if (mesData.Month.Equals(dataRelatorio.Month))
                                            {
                                                string operacao = reader["Operacao"].ToString();
                                                int quantidadeReceptivo = int.Parse(reader["QuantidadeReceptivo"].ToString());
                                                int quantidadeAtivo = int.Parse(reader["QuantidadeAtivo"].ToString());

                                                RelatorioNumeroChatsBean bean = new RelatorioNumeroChatsBean
                                                {
                                                    Mes = mesData.ToString("MM/yyyy"),
                                                    Operacao = $"{dadosSQL.NumeroTelefone} - {operacao}",
                                                    QuantidadeAtivo = quantidadeAtivo,
                                                    QuantidadeReceptivo = quantidadeReceptivo
                                                };

                                                listaCompletaNumeroChats.Add(bean);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Console.WriteLine($" ");
                        Console.WriteLine($"Dados obtidos com sucesso.");
                        _logger.Info("Consulta realizada com sucesso, quantidade de atendimentos chats armazenado na lista!");

                        CriarArquivo(listaCompletaNumeroChats, "Relação Chats");
                    }
                    catch (Exception ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"Erro ao obter dados {ex.Message}", Console.ForegroundColor);
                        Console.ResetColor();
                        _logger.Error(ex.Message, $"Erro ao tentar buscar quantidade de atendimentos de chat");
                    }
                    #endregion
                }

                if (numeroResposta == 2)
                {
                    #region relação faturável
                    try
                    {
                        foreach (var dadosSQL in listaDeStringsSql)
                        {
                            if (dadosSQL.Nome.Equals("Prevent Senior"))
                                continue;

                            Console.WriteLine($"Obtendo dados de: {dadosSQL.Nome}");
                            using (var connection = new SqlConnection(dadosSQL.StringConexao))
                            {
                                using (var command = new SqlCommand())
                                {
                                    connection.Open();

                                    command.Connection = connection;
                                    command.CommandType = System.Data.CommandType.Text;

                                    command.CommandText = @$"SELECT 
                                                                '{dadosSQL.Nome}' AS 'CLIENTE',
                                                            	_login AS 'LOGIN',
                                                            	_name AS 'NOME', 
                                                            	CASE 
                                                            		WHEN _active = 1 THEN 'Ativo'
                                                            		ELSE 'Inativo'
                                                            	END AS 'STATUS',
                                                            	CASE 
                                                            		WHEN _billable = 1 THEN 'SIM'
                                                            		ELSE 'NÃO'
                                                            	END AS 'FATURÁVEL' 
                                                            FROM  
                                                            	users WITH(NOLOCK)";

                                    command.CommandTimeout = 0;

                                    using (var reader = command.ExecuteReader())
                                    {
                                        while (reader.Read())
                                        {
                                            string cliente = reader["CLIENTE"].ToString();
                                            string login = reader["LOGIN"].ToString();
                                            string nome = reader["NOME"].ToString();
                                            string status = reader["STATUS"].ToString();
                                            string faturavel = reader["FATURÁVEL"].ToString();

                                            RelatorioUsuariosFaturaveis bean = new RelatorioUsuariosFaturaveis
                                            {
                                                Cliente = cliente,
                                                Login = login,
                                                Nome = nome,
                                                Status = status,
                                                Faturavel = faturavel
                                            };

                                            listaCompletaUsuariosFaturaveis.Add(bean);
                                        }
                                    }
                                }
                            }
                        }
                        Console.WriteLine($" ");
                        Console.WriteLine($"Dados obtidos com sucesso.");
                        _logger.Info("Consulta realizada com sucesso, quantidade de atendimentos chats armazenado na lista!");


                        CriarArquivo(listaCompletaUsuariosFaturaveis);
                    }
                    catch (Exception ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"Erro ao obter dados {ex.Message}", Console.ForegroundColor);
                        Console.ResetColor();
                        _logger.Error(ex.Message, $"Erro ao tentar buscar quantidade de atendimentos de chat");
                    }
                    #endregion
                }

                if (numeroResposta == 3)
                {
                    #region relação chats
                    try
                    {
                        foreach (var dadosSQL in listaDeStringsSql)
                        {
                            Console.WriteLine($"Obtendo dados de: {dadosSQL.Nome}");
                            using (var connection = new SqlConnection(dadosSQL.StringConexao))
                            {
                                using (var command = new SqlCommand())
                                {
                                    connection.Open();

                                    command.Connection = connection;
                                    command.CommandType = System.Data.CommandType.Text;

                                    command.CommandText = $@"                                    	
                                        select [Operacao] = @base,	
                                        	   [Mes] = FORMAT(Cm._createdAt, 'yyyy/MM'), 
                                        	   [QuantidadeRecebida] = SUM(CASE WHEN Cm._fromType = 'Agent' THEN 1 ELSE 0 END),
                                        	   [QuantidadeEnviada] = SUM(CASE WHEN Cm._fromType = 'User' THEN 1 ELSE 0 END),
                                        	   [QuantidadeSistema] = SUM(CASE WHEN Cm._fromType = 'System' THEN 1 ELSE 0 END)
                                        	from chat_messages Cm WITH(NOLOCK)
                                        		where Cm._createdAt > '2015-01-01' 
                                        		group by FORMAT(Cm._createdAt, 'yyyy/MM')
                                        		order by FORMAT(Cm._createdAt, 'yyyy/MM') asc										
                                                ";

                                    command.CommandTimeout = 0;

                                    command.Parameters.Add("@base", System.Data.SqlDbType.VarChar, 255).Value = dadosSQL.Nome;

                                    using (var reader = command.ExecuteReader())
                                    {
                                        while (reader.Read())
                                        {
                                            string operacao = reader["Operacao"].ToString();


                                            RelatorioQuantidadeMensagem bean = new RelatorioQuantidadeMensagem
                                            {
                                                Mes = reader["Mes"].ToString(),
                                                Operacao = $"{operacao} - {dadosSQL.NumeroTelefone}",
                                                QuantidadeEnviada = int.Parse(reader["QuantidadeEnviada"].ToString()),
                                                QuantidadeRecebidas = int.Parse(reader["QuantidadeRecebida"].ToString()),
                                                QuantidadeSistema = int.Parse(reader["QuantidadeSistema"].ToString())

                                            };

                                            listaCompletaQuantidadeMensages.Add(bean);
                                        }
                                    }
                                }
                            }
                        }
                        Console.WriteLine($" ");
                        Console.WriteLine($"Dados obtidos com sucesso.");
                        _logger.Info("Consulta realizada com sucesso, quantidade de mensagens por produto armazenado na lista!");

                        CriarArquivo(listaCompletaQuantidadeMensages, "QuantidadeMensagens");
                    }
                    catch (Exception ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"Erro ao obter dados {ex.Message}", Console.ForegroundColor);
                        Console.ResetColor();
                        _logger.Error(ex.Message, $"Erro ao tentar buscar quantidade de mensagens por produto");
                    }
                    #endregion
                }

                if (numeroResposta == 4)
                {
                    #region usuariosVolt
                    try
                    {
                        foreach (var dadosSQL in listaDeStringsSqlVolt)
                        {
                            Console.WriteLine($"Obtendo dados de: {dadosSQL.Nome}");
                            using (var connection = new SqlConnection(dadosSQL.StringConexao))
                            {
                                using (var command = new SqlCommand())
                                {
                                    connection.Open();

                                    command.Connection = connection;
                                    command.CommandType = System.Data.CommandType.Text;

                                    command.CommandText = @$"SELECT 
                                                                '{dadosSQL.Nome}' AS 'CLIENTE',
                                                            u.Name AS 'NOME',
                                                        	u.Email AS 'EMAIL', 
                                                        	CASE 
                                                        		WHEN u.Active = 1 THEN 'Ativo'
                                                        		ELSE 'Inativo'
                                                        	END AS 'STATUS', 
                                                        	CASE
                                                        		WHEN u.Deleted = 1 THEN 'SIM'
                                                        		ELSE 'NÃO'
                                                        	END AS 'DELETADO'
                                                        from dbo.AspNetUsers u";

                                    command.CommandTimeout = 0;

                                    using (var reader = command.ExecuteReader())
                                    {
                                        while (reader.Read())
                                        {
                                            RelatorioUsuariosFaturaveisVolt bean = new RelatorioUsuariosFaturaveisVolt
                                            {
                                                Cliente = reader["CLIENTE"].ToString(),
                                                Nome = reader["NOME"].ToString(),
                                                Email = reader["EMAIL"].ToString(),
                                                Status = reader["STATUS"].ToString(),
                                                Deletado = reader["DELETADO"].ToString()
                                            };

                                            listaCompletaUsuariosFaturaveisVolt.Add(bean);
                                        }
                                    }
                                }
                            }
                        }
                        Console.WriteLine($" ");
                        Console.WriteLine($"Dados obtidos com sucesso.");
                        _logger.Info("Consulta realizada com sucesso, quantidade de atendimentos chats armazenado na lista!");


                        CriarArquivo(listaCompletaUsuariosFaturaveisVolt, "FaturaveisVolt");
                    }
                    catch (Exception ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"Erro ao obter dados {ex.Message}", Console.ForegroundColor);
                        Console.ResetColor();
                        _logger.Error(ex.Message, $"Erro ao tentar buscar quantidade de atendimentos de chat");
                    }
                    #endregion usuariosVolt
                }

                if (numeroResposta == 5)
                {


                    #region QuantidadesChatApi
                    try
                    {
                        string connectionStringC1Tools = "Server=10.0.0.4,1433;Initial Catalog=c1_tools;Persist Security Info=True;User ID=c1app_whatsapp;Password=prAraj!&huwesa6u";
                        RelatorioQuantidadeMensagensChatApi bean = new RelatorioQuantidadeMensagensChatApi();

                        Console.WriteLine($"Obtendo dados de Quantidade de mensagens ChatAPi");
                        using (var connection = new SqlConnection(connectionStringC1Tools))
                        {
                            using (var command = new SqlCommand())
                            {
                                connection.Open();

                                command.Connection = connection;
                                command.CommandType = System.Data.CommandType.Text;

                                command.CommandText = @$"SELECT 
                                                        FORMAT (w._createdat, 'MMMM yyyy') as 'Data',
                                                        ua._description as 'Cliente', 
                                                        ac._uid as 'Numero', 
                                                        [Recebidas] = SUM(CASE WHEN w._apiname = 'ChatReceive' THEN 1 ELSE 0 END),
                                                        [Enviadas] = SUM(CASE WHEN w._apiname = 'ChatSend' THEN 1 ELSE 0 END),
                                                        [HSM] = SUM(CASE WHEN w._apiname = 'ChatSendHsm' THEN 1 ELSE 0 END)
                                                        from api_wamessages w with(NOLOCK) 
                                                        LEFT JOIN api_userapis ua with(NOLOCK)  on ua._userid = w._apiuserid
                                                        left join api_useraccounts ac with(NOLOCK)  on ac._id = ua._useraccountid
                                                        where ua._useraccountid in (20,18,28,15,29,30,5,43,52,55,25) AND (w._status <> 'failed' AND w._apierror = 0)  AND
                                                        w._createdat BETWEEN '2022-04-01' AND '2022-04-30 23:59:59'
                                                        group by FORMAT (w._createdat, 'MMMM yyyy') ,ua._description, ac._uid
                                                        order by ua._description
                                                        ";

                                command.CommandTimeout = 0;

                                using (var reader = command.ExecuteReader())
                                {
                                    while (reader.Read())
                                    {
                                        bean = new RelatorioQuantidadeMensagensChatApi
                                        {
                                            Data = reader["Data"].ToString(),
                                            Cliente = reader["Cliente"].ToString(),
                                            Numero = $"[{reader["Numero"].ToString()}]",
                                            Recebidas = reader["Recebidas"].ToString(),
                                            Enviadas = reader["Enviadas"].ToString(),
                                            HSM = reader["HSM"].ToString()
                                        };

                                        listaCompletaQuantidadeMensagensChatApi.Add(bean);
                                    }
                                }
                            }
                        }

                        Console.WriteLine($" ");
                        Console.WriteLine($"Dados obtidos com sucesso.");
                        _logger.Info("Consulta realizada com sucesso, quantidade de atendimentos chats armazenado na lista!");


                        CriarArquivo(listaCompletaQuantidadeMensagensChatApi, "QuantidadeMensagensChatApi");
                    }
                    catch (Exception ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"Erro ao obter dados {ex.Message}", Console.ForegroundColor);
                        Console.ResetColor();
                        _logger.Error(ex.Message, $"Erro ao tentar buscar quantidade de atendimentos de chat");
                    }
                    #endregion QuantidadesChatApi
                }

            }


        }

        private static void CriarArquivo(List<RelatorioNumeroChatsBean> listaCompleta, string pasta)
        {
            if (listaCompleta.Count > 0)
            {
                try
                {
                    Console.WriteLine($"Iniciando criação de arquivo!");

                    string mes = listaCompleta.AsQueryable().Select(x => x.Mes).FirstOrDefault();
                    string caminhoCompletoAnexo = Path.Combine(@$"D:\RelatoriosC1\{pasta}", $"Chats {mes.Replace("/", "-")}.csv");
                    string caminhoDiretorioAnexo = Path.GetDirectoryName(caminhoCompletoAnexo);

                    if (!Directory.Exists(caminhoDiretorioAnexo))
                        Directory.CreateDirectory(caminhoDiretorioAnexo);

                    var configurationCsv = new CsvHelper.Configuration.CsvConfiguration(new System.Globalization.CultureInfo("pt-BR"))
                    {
                        Delimiter = ";",

                    };

                    using (var writer = new StreamWriter(caminhoCompletoAnexo, false, Encoding.UTF8))
                    using (var csv = new CsvWriter(writer, configurationCsv))
                    {
                        csv.WriteRecords(listaCompleta);
                    }

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"Relatório criado com sucesso!");
                    Console.ResetColor();
                    _logger.Info("Relatório criado com sucesso!");
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"Erro ao criar arquivo: {ex.Message}", Console.ForegroundColor);
                    Console.ResetColor();
                    _logger.Error(ex.Message, $"Erro ao tentar criar relatório arquivo csv");
                }
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Nenhum dado na lista", Console.ForegroundColor);
                Console.ResetColor();
                _logger.Info("Nenhum dado encontrado na lista!");
            }
        }
        private static void CriarArquivo(List<RelatorioQuantidadeMensagem> listaCompleta, string pasta)
        {
            if (listaCompleta.Count > 0)
            {
                try
                {
                    Console.WriteLine($"Iniciando criação de arquivo!");

                    string mes = listaCompleta.AsQueryable().Select(x => x.Mes).FirstOrDefault();
                    string caminhoCompletoAnexo = Path.Combine(@$"D:\RelatoriosC1\{pasta}", $"{mes.Replace("/", "-")}.csv");
                    string caminhoDiretorioAnexo = Path.GetDirectoryName(caminhoCompletoAnexo);

                    if (!Directory.Exists(caminhoDiretorioAnexo))
                        Directory.CreateDirectory(caminhoDiretorioAnexo);

                    var configurationCsv = new CsvHelper.Configuration.CsvConfiguration(new System.Globalization.CultureInfo("pt-BR"))
                    {
                        Delimiter = ";",

                    };

                    using (var writer = new StreamWriter(caminhoCompletoAnexo, false, Encoding.UTF8))
                    using (var csv = new CsvWriter(writer, configurationCsv))
                    {
                        csv.WriteRecords(listaCompleta);
                    }

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"Relatório criado com sucesso!");
                    Console.ResetColor();
                    _logger.Info("Relatório criado com sucesso!");
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"Erro ao criar arquivo: {ex.Message}", Console.ForegroundColor);
                    Console.ResetColor();
                    _logger.Error(ex.Message, $"Erro ao tentar criar relatório arquivo csv");
                }
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Nenhum dado na lista", Console.ForegroundColor);
                Console.ResetColor();
                _logger.Info("Nenhum dado encontrado na lista!");
            }
        }
        private static void CriarArquivo(List<RelatorioUsuariosFaturaveisVolt> listaCompleta, string pasta)
        {
            if (listaCompleta.Count > 0)
            {
                try
                {
                    Console.WriteLine($"Iniciando criação de arquivo!");

                    string caminhoCompletoAnexo = Path.Combine(@$"D:\RelatoriosC1\{pasta}", $"VOLT - Usuários Faturáveis - {DateTime.Now.Month}-{DateTime.Now.Year}.csv");
                    string caminhoDiretorioAnexo = Path.GetDirectoryName(caminhoCompletoAnexo);

                    if (!Directory.Exists(caminhoDiretorioAnexo))
                        Directory.CreateDirectory(caminhoDiretorioAnexo);

                    var configurationCsv = new CsvHelper.Configuration.CsvConfiguration(new System.Globalization.CultureInfo("pt-BR"))
                    {
                        Delimiter = ";",

                    };

                    using (var writer = new StreamWriter(caminhoCompletoAnexo, false, Encoding.UTF8))
                    using (var csv = new CsvWriter(writer, configurationCsv))
                    {
                        csv.WriteRecords(listaCompleta);
                    }

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"Relatório criado com sucesso!");
                    Console.ResetColor();
                    _logger.Info("Relatório criado com sucesso!");
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"Erro ao criar arquivo: {ex.Message}", Console.ForegroundColor);
                    Console.ResetColor();
                    _logger.Error(ex.Message, $"Erro ao tentar criar relatório arquivo csv");
                }
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Nenhum dado na lista", Console.ForegroundColor);
                Console.ResetColor();
                _logger.Info("Nenhum dado encontrado na lista!");
            }
        }
        private static void CriarArquivo(List<RelatorioQuantidadeMensagensChatApi> listaCompleta, string pasta)
        {
            if (listaCompleta.Count > 0)
            {
                try
                {
                    Console.WriteLine($"Iniciando criação de arquivo!");

                    string caminhoCompletoAnexo = Path.Combine(@$"D:\RelatoriosC1\{pasta}", $"Quantidade mensagens CHATAPI - {DateTime.Now.Month}-{DateTime.Now.Year}.csv");
                    string caminhoDiretorioAnexo = Path.GetDirectoryName(caminhoCompletoAnexo);

                    if (!Directory.Exists(caminhoDiretorioAnexo))
                        Directory.CreateDirectory(caminhoDiretorioAnexo);

                    var configurationCsv = new CsvHelper.Configuration.CsvConfiguration(new System.Globalization.CultureInfo("pt-BR"))
                    {
                        Delimiter = ";",

                    };
                    
                    using (var writer = new StreamWriter(caminhoCompletoAnexo, false, Encoding.UTF8))
                    using (var csv = new CsvWriter(writer, configurationCsv))
                    {
                        csv.WriteRecords(listaCompleta);
                    }

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"Relatório criado com sucesso!");
                    Console.ResetColor();
                    _logger.Info("Relatório criado com sucesso!");
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"Erro ao criar arquivo: {ex.Message}", Console.ForegroundColor);
                    Console.ResetColor();
                    _logger.Error(ex.Message, $"Erro ao tentar criar relatório arquivo csv");
                }
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Nenhum dado encontrado", Console.ForegroundColor);
                Console.ResetColor();
                _logger.Info("Nenhum dado encontrado!");
            }
        }
        private static void CriarArquivo(List<RelatorioUsuariosFaturaveis> listaCompleta)
        {
            if (listaCompleta.Count > 0)
            {
                try
                {
                    Console.WriteLine($"Iniciando criação de arquivo!");

                    string caminhoCompletoAnexo = Path.Combine(@"D:\RelatoriosC1\Relação Chats\Faturaveis", $"SMARTCHAT - Usuários Faturáveis - {DateTime.Now.Month}-{DateTime.Now.Year}.csv");
                    string caminhoDiretorioAnexo = Path.GetDirectoryName(caminhoCompletoAnexo);

                    if (!Directory.Exists(caminhoDiretorioAnexo))
                        Directory.CreateDirectory(caminhoDiretorioAnexo);

                    var configurationCsv = new CsvHelper.Configuration.CsvConfiguration(new System.Globalization.CultureInfo("pt-BR"))
                    {
                        Delimiter = ";",

                    };

                    using (var writer = new StreamWriter(caminhoCompletoAnexo, false, Encoding.UTF8))
                    using (var csv = new CsvWriter(writer, configurationCsv))
                    {
                        csv.WriteRecords(listaCompleta);
                    }

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"Relatório criado com sucesso!");
                    Console.ResetColor();
                    _logger.Info("Relatório criado com sucesso!");

                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"Erro ao criar arquivo: {ex.Message}", Console.ForegroundColor);
                    Console.ResetColor();
                    _logger.Error(ex.Message, $"Erro ao tentar criar relatório arquivo csv");
                }
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Nenhum dado na lista", Console.ForegroundColor);
                Console.ResetColor();
                _logger.Info("Nenhum dado encontrado na lista!");
            }
        }

    }
}
