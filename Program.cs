using System.Diagnostics;
using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;

namespace LTreeApp;

internal static class Program
{
    // Configuration parameters - can be adjusted
    private static int _companyCount = 20;
    private static int _subsystemsPerCompany = 25;
    private static int _webIdsPerSubsystem = 30;
    private static int _playersPerWebId = 20;
    private static int _recordsPerPlayer = 40;

    // Batch sizes for bulk inserts
    private static int _batchSize = 2000;

    // Connection string - replace with your actual connection string
    private static string _connectionString = "Host=localhost:5435;Database=demo_user;Username=demo_user;Password=demo@123";

    static async Task Main(string[] args)
    {
        await GetSetting();

        var stopwatch = new Stopwatch();
        stopwatch.Start();

        Console.WriteLine("Starting data generation...");
        
        // Clear existing data if needed
        await ClearExistingData();
        
        // Generate flattened data
        await GenerateFlatteningRecords();

        // Generate hierarchical data (v1)
        await GenerateHierarchicalData();
        
        // Generate ltree hierarchical data
        await GenerateLtreeHierarchicalData();
        
        stopwatch.Stop();
        Console.WriteLine($"Data generation completed in {stopwatch.ElapsedMilliseconds / 1000.0} seconds");
    }

    private static async Task GetSetting()
    {
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())  // 設定目前目錄為基底目錄
            .AddJsonFile("appsetting.json", optional: false, reloadOnChange: true)  // 加載 appsettings.json
            .Build();

        var connectionString = configuration["ConnectionString"];
        if(string.IsNullOrWhiteSpace(connectionString) == false)
            _connectionString = connectionString;
        
        var companyCountString = configuration["CompanyCount"];
        if(string.IsNullOrWhiteSpace(companyCountString) == false && int.TryParse(companyCountString, out var companyCount) == false)
            _companyCount = companyCount;
        
        var subsystemsPerCompanyString = configuration["SubsystemsPerCompany"];
        if(string.IsNullOrWhiteSpace(subsystemsPerCompanyString) == false && int.TryParse(subsystemsPerCompanyString, out var subsystemsPerCompany) == false)
            _subsystemsPerCompany = subsystemsPerCompany;
        
        var webIdsPerSubsystemString = configuration["WebIdsPerSubsystem"];
        if(string.IsNullOrWhiteSpace(webIdsPerSubsystemString) == false && int.TryParse(webIdsPerSubsystemString, out var webIdsPerSubsystem) == false)
            _webIdsPerSubsystem = webIdsPerSubsystem;
        
        var playersPerWebIdString = configuration["PlayersPerWebId"];
        if(string.IsNullOrWhiteSpace(playersPerWebIdString) == false && int.TryParse(playersPerWebIdString, out var playersPerWebId) == false)
            _playersPerWebId = playersPerWebId;
        
        var recordPerPlayerString = configuration["RecordsPerPlayer"];
        if(string.IsNullOrWhiteSpace(recordPerPlayerString) == false && int.TryParse(recordPerPlayerString, out var recordPerPlayer) == false)
            _recordsPerPlayer = recordPerPlayer;
        
        var batchSizeString = configuration["BatchSize"];
        if(string.IsNullOrWhiteSpace(batchSizeString) == false && int.TryParse(batchSizeString, out var batchSize) == false)
            _batchSize = batchSize;
    }

    private static async Task ClearExistingData()
    {
        Console.WriteLine("Clearing existing data...");

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();
        
        await connection.ExecuteAsync(@"
            TRUNCATE flattening_records RESTART IDENTITY CASCADE;
            TRUNCATE hierarchy_relation RESTART IDENTITY CASCADE;
            TRUNCATE hierarchy_player RESTART IDENTITY CASCADE;
            TRUNCATE hierarchy_records RESTART IDENTITY CASCADE;
            TRUNCATE hierarchy_relation_ltree RESTART IDENTITY CASCADE;
            TRUNCATE hierarchy_player_ltree RESTART IDENTITY CASCADE;
            TRUNCATE hierarchy_records_ltree RESTART IDENTITY CASCADE;
            
            INSERT INTO hierarchy_relation (parent_id, level, name) VALUES (0, 0, 'all');
            INSERT INTO hierarchy_relation_ltree (path, level, name) VALUES ('1', 0, 'all');
        ");
    }

    private static async Task GenerateFlatteningRecords()
    {
        Console.WriteLine("Generating flattening records...");
        var sw = Stopwatch.StartNew();

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();
        
        var flattenedRecords = new List<(string Company, string SubSystem, string WebId, string PlayerName, int Balance)>();
        var random = new Random();
        
        // Pre-generate all company/subsystem/webid/player combinations
        for (var c = 1; c <= _companyCount; c++)
        {
            var company = $"Company{c}";
            
            for (var s = 1; s <= _subsystemsPerCompany; s++)
            {
                var subsystem = $"Subsystem{s}";
                
                for (var w = 1; w <= _webIdsPerSubsystem; w++)
                {
                    var webId = $"WebId{w}";
                    
                    for (var p = 1; p <= _playersPerWebId; p++)
                    {
                        var playerName = $"Player{c}_{s}_{w}_{p}";
                        for (var r = 1; r <= _recordsPerPlayer; r++)
                        {
                            // var balance = random.Next(1, 10000);
                            const int balance = 10;
                            flattenedRecords.Add((company, subsystem, webId, playerName, balance));
                        }
                    }
                }
            }
        }
        

        if (flattenedRecords.Count > 0)
        {
            await BulkInsertFlatteningRecords(connection, flattenedRecords);
        }
        
        sw.Stop();
        Console.WriteLine($"Flattening records generated in {sw.ElapsedMilliseconds / 1000.0} seconds");
        return;
        static async Task BulkInsertFlatteningRecords(NpgsqlConnection connection, List<(string Company, string SubSystem, string WebId, string PlayerName, int Balance)> records)
        {
            for (var i = 0; i < records.Count; i += _batchSize)
            {
                var batch = records.Skip(i).Take(_batchSize).ToList();
                var insertString = string.Join(",", batch.Select(r => $"('{r.Company}','{r.SubSystem}','{r.WebId}','{r.PlayerName}','{r.Balance}')"));
                await connection.ExecuteAsync($"INSERT INTO flattening_records (company, sub_system, web_id, player_name, balance) VALUES {insertString}");
            }
        }
    }

    private static async Task GenerateHierarchicalData()
    {
        Console.WriteLine("Generating hierarchical data (v1)...");
        var sw = Stopwatch.StartNew();

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();
        
        // Get the 'all' root node id
        var allId = await connection.ExecuteScalarAsync<int>("SELECT id FROM hierarchy_relation WHERE name = 'all'");
        
        // In-memory dictionary to track relation ids
        var relationIds = new Dictionary<string, int>();
        relationIds["all"] = allId;
        
        // Prepare company data
        var companies = new List<(int ParentId, int Level, string Name)>();
        for (var c = 1; c <= _companyCount; c++)
        {
            companies.Add((allId, 1, $"Company{c}"));
        }
        
        // Bulk insert companies and get their IDs
        var companyIds = await BulkInsertHierarchyRelations(connection, companies);
        
        // Map company names to their IDs
        for (var c = 0; c < companies.Count; c++)
        {
            relationIds[companies[c].Name] = companyIds[c];
        }
        
        // For each company, prepare subsystems
        for (var c = 1; c <= _companyCount; c++)
        {
            var companyName = $"Company{c}";
            var companyId = relationIds[companyName];
            
            // Prepare subsystem data
            var subsystems = new List<(int ParentId, int Level, string Name)>();
            for (var s = 1; s <= _subsystemsPerCompany; s++)
            {
                subsystems.Add((companyId, 2, $"Subsystem{s}"));
            }
            
            // Bulk insert subsystems and get their IDs
            var subsystemIds = await BulkInsertHierarchyRelations(connection, subsystems);
            
            // Map subsystem names to their IDs
            for (var s = 0; s < subsystems.Count; s++)
            {
                relationIds[$"{companyName}_{subsystems[s].Name}"] = subsystemIds[s];
            }
            
            // For each subsystem, prepare webids
            for (var s = 1; s <= _subsystemsPerCompany; s++)
            {
                var subsystemName = $"Subsystem{s}";
                var subsystemId = relationIds[$"{companyName}_{subsystemName}"];
                
                // Prepare webid data
                var webIds = new List<(int ParentId, int Level, string Name)>();
                for (var w = 1; w <= _webIdsPerSubsystem; w++)
                {
                    webIds.Add((subsystemId, 3, $"WebId{w}"));
                }
                
                // Bulk insert webids and get their IDs
                var webidIds = await BulkInsertHierarchyRelations(connection, webIds);
                
                // Map webid names to their IDs
                for (var w = 0; w < webIds.Count; w++)
                {
                    relationIds[$"{companyName}_{subsystemName}_{webIds[w].Name}"] = webidIds[w];
                }
                
                // For each webid, generate players and records
                for (var w = 1; w <= _webIdsPerSubsystem; w++)
                {
                    var webIdName = $"WebId{w}";
                    var webIdId = relationIds[$"{companyName}_{subsystemName}_{webIdName}"];
                    
                    // Prepare player data
                    var players = new List<(int RelationId, string PlayerName)>();
                    for (var p = 1; p <= _playersPerWebId; p++)
                    {
                        players.Add((webIdId, $"Player{c}_{s}_{w}_{p}"));
                    }
                    
                    // Bulk insert players and get their IDs/names
                    var playerIdMap = await BulkInsertHierarchyPlayers(connection, players);
                    
                    // Prepare record data
                    var random = new Random();
                    var records = new List<(int RelationId, int PlayerId, int Balance)>();
                    
                    foreach (var playerEntry in playerIdMap)
                    {
                        var playerName = playerEntry.Key;
                        var playerId = playerEntry.Value;
                        
                        for (var r = 1; r <= _recordsPerPlayer; r++)
                        {
                            //var balance = random.Next(1, 10000);
                            const int balance = 10;
                            records.Add((webIdId, playerId, balance));
                        }
                    }
                    
                    // Bulk insert records
                    await BulkInsertHierarchyRecords(connection, records);
                }
            }
        }
        
        sw.Stop();
        
        Console.WriteLine($"Hierarchical data generated in {sw.ElapsedMilliseconds / 1000.0} seconds");
        return;


        static async Task BulkInsertHierarchyRecords(NpgsqlConnection connection, List<(int RelationId, int PlayerId, int Balance)> records)
        {
            if (records.Count == 0)
                return;

            for (var i = 0; i < records.Count; i += _batchSize)
            {
                var batch = records.Skip(i).Take(_batchSize).ToList();
                var insertString = string.Join(",", batch.Select(r => $"('{r.RelationId}','{r.PlayerId}','{r.Balance}')"));
                await connection.ExecuteAsync($"INSERT INTO hierarchy_records (relation_id, player_id, balance) VALUES {insertString}");
            }
        }
        
        static async Task<Dictionary<string, int>> BulkInsertHierarchyPlayers(NpgsqlConnection connection, List<(int RelationId, string PlayerName)> players)
        {
            var result = new Dictionary<string, int>();
        
            if (players.Count == 0)
                return result;

            var insertedPlayers = new List<(int Id, string PlayerName)>();
            for (var i = 0; i < players.Count; i += _batchSize)
            {
                var batch = players.Skip(i).Take(_batchSize).ToList();
                var insertString = string.Join(",", batch.Select(r => $"('{r.RelationId}','{r.PlayerName}')"));
                var insertResult = await connection.QueryAsync<(int Id, string PlayerName)>(
                    $"INSERT INTO hierarchy_player (relation_id, player_name) VALUES {insertString} RETURNING id, player_name");
                insertedPlayers.AddRange(insertResult);
            }
        
            foreach (var player in insertedPlayers)
            {
                result[player.PlayerName] = player.Id;
            }

            return result;
        }
        
        static async Task<List<int>> BulkInsertHierarchyRelations(NpgsqlConnection connection, List<(int ParentId, int Level, string Name)> relations)
        {
            if (relations.Count == 0)
                return new List<int>();
        
            var totalIds = new List<int>();
            for (var i = 0; i < relations.Count; i += _batchSize)
            {
                var batch = relations.Skip(i).Take(_batchSize).ToList();
                var insertString = string.Join(",", batch.Select(r => $"('{r.ParentId}','{r.Level}','{r.Name}')"));
                var ids = await connection.QueryAsync<int>($"INSERT INTO hierarchy_relation (parent_id, level, name) VALUES {insertString} RETURNING id");
                totalIds.AddRange(ids);
            }
            return totalIds;
        }
    }

    private static async Task GenerateLtreeHierarchicalData()
    {
        Console.WriteLine("Generating ltree hierarchical data...");
        var sw = Stopwatch.StartNew();

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();
        
        // Get the 'all' root node
        var allNode = await connection.QueryFirstAsync<(int Id, string Path)>(
            "SELECT id, path::text FROM hierarchy_relation_ltree WHERE name = 'all'");
        
        // In-memory dictionaries to track paths and ids
        var nodePaths = new Dictionary<string, string>();
        var nodeIds = new Dictionary<string, int>();
        nodePaths["all"] = allNode.Path;
        nodeIds["all"] = allNode.Id;
        
        // Insert companies
        for (var c = 1; c <= _companyCount; c++)
        {
            var companyName = $"Company{c}";
            var companyNode = await InsertLtreeHierarchyRelation(connection, 1, allNode.Path, companyName);
            nodePaths[companyName] = companyNode.Path;
            nodeIds[companyName] = companyNode.Id;
            
            // Insert subsystems for this company
            for (var s = 1; s <= _subsystemsPerCompany; s++)
            {
                var subsystemName = $"Subsystem{s}";
                var subsystemKey = $"{companyName}_{subsystemName}";
                var subsystemNode = await InsertLtreeHierarchyRelation(connection, 2, companyNode.Path, subsystemName);
                nodePaths[subsystemKey] = subsystemNode.Path;
                nodeIds[subsystemKey] = subsystemNode.Id;
                
                // Insert webids for this subsystem
                for (var w = 1; w <= _webIdsPerSubsystem; w++)
                {
                    var webIdName = $"WebId{w}";
                    var webIdKey = $"{subsystemKey}_{webIdName}";
                    var webIdNode = await InsertLtreeHierarchyRelation(connection, 3, subsystemNode.Path, webIdName);
                    nodePaths[webIdKey] = webIdNode.Path;
                    nodeIds[webIdKey] = webIdNode.Id;
                    
                    // Create batch collections for players and records
                    var players = new List<(int RelationId, string PlayerName)>();
                    var records = new List<(string Path, int PlayerId, int Balance)>();
                    var random = new Random();
                    
                    // Generate players for this webid
                    for (var p = 1; p <= _playersPerWebId; p++)
                    {
                        var playerName = $"Player{c}_{s}_{w}_{p}";
                        players.Add((webIdNode.Id, playerName));
                    }
                    
                    // Bulk insert players
                    var playerIdMap = await BulkInsertLtreeHierarchyPlayers(connection, players);
                    
                    // Generate records for these players
                    foreach (var player in players)
                    {
                        var playerId = playerIdMap[player.PlayerName];
                        
                        for (var r = 1; r <= _recordsPerPlayer; r++)
                        {
                            // var balance = random.Next(1, 10000);
                            const int balance = 10;
                            records.Add((webIdNode.Path, playerId, balance));
                        }
                    }
                    
                    // Bulk insert records
                    await BulkInsertLtreeHierarchyRecords(connection, records);
                }
            }
        }
        
        sw.Stop();
        Console.WriteLine($"Ltree hierarchical data generated in {sw.ElapsedMilliseconds / 1000.0} seconds");
        return;


        static async Task<(int Id, string Path)> InsertLtreeHierarchyRelation(NpgsqlConnection connection, int level, string parentPath, string name)
        {
            // const string sql = @"
            // INSERT INTO hierarchy_relation_ltree (level, path, name)
            // VALUES (@Level, @ParentPath || '.' || (SELECT COALESCE(MAX(id), 0) + 1 FROM hierarchy_relation_ltree), @Name)
            // RETURNING id, path::text";
            
            const string sql = @"
    INSERT INTO hierarchy_relation_ltree (level, path, name)
    VALUES (@Level, (@ParentPath || '.' || (SELECT COALESCE(MAX(id), 0) + 1 FROM hierarchy_relation_ltree))::ltree, @Name)
    RETURNING id, path::text";

            

            return await connection.QueryFirstAsync<(int Id, string Path)>(sql, new { Level = level, ParentPath = parentPath, Name = name });
        }
        
        
        static async Task<Dictionary<string, int>> BulkInsertLtreeHierarchyPlayers(NpgsqlConnection connection, List<(int RelationId, string PlayerName)> players)
        {
            var result = new Dictionary<string, int>();
        
            if (players.Count == 0)
                return result;
            
            var insertedPlayers = new List<(int Id, string PlayerName)>();
            for (var i = 0; i < players.Count; i += _batchSize)
            {
                var batch = players.Skip(i).Take(_batchSize).ToList();
                var insertString = string.Join(",", batch.Select(r => $"('{r.RelationId}','{r.PlayerName}')"));
                var insertResult = await connection.QueryAsync<(int Id, string PlayerName)>($"INSERT INTO hierarchy_player_ltree (relation_id, player_name) VALUES {insertString} RETURNING id, player_name");
                insertedPlayers.AddRange(insertResult);
            }
        
            foreach (var player in insertedPlayers)
            {
                result[player.PlayerName] = player.Id;
            }
        
            return result;
        }
        
        static async Task BulkInsertLtreeHierarchyRecords(NpgsqlConnection connection, List<(string Path, int PlayerId, int Balance)> records)
        {
            if (records.Count == 0)
                return;
            
            for (var i = 0; i < records.Count; i += _batchSize)
            {
                var batch = records.Skip(i).Take(_batchSize).ToList();
                var insertString = string.Join(",", batch.Select(r => $"('{r.Path}','{r.PlayerId}','{r.Balance}')"));
                await connection.ExecuteAsync($"INSERT INTO hierarchy_records_ltree (path, player_id, balance) VALUES {insertString}");
            }
        }
    }
    
}