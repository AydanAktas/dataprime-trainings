using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Bogus;
using Microsoft.Azure.Cosmos;
using System.IO;
using System.Text.Json;

//string endpoint = "https://cosmosdb-module1-yoo4baps37hu2.documents.azure.com:443/";
//string key = "KNZY8nLpJzvSlEz9quyCO7ArntZYcZq273CcwgJOfVx2qb04mQuO37UxEdD1RBKNKVy1aLA9b1UhACDbscqAcA==";

internal class Program
{
    private static async Task Main(string[] args)
    {
        string endpoint = args[0];
        string key = args[1];

        CosmosClientOptions options = new()
        {
            AllowBulkExecution = true
        };

        CosmosClient client = new(endpoint, key, options);

        Container container = client.GetContainer("Address", "Address");

        string addressJsonData = File.ReadAllText("Address.json");

        var addresses = JsonSerializer.Deserialize<List<Address>>(addressJsonData);

        List<Task> concurrentTasks = new List<Task>();

        foreach (Address address in addresses)
        {
            concurrentTasks.Add(
                container.CreateItemAsync(address, new PartitionKey(address.PostalCode))
            );
        }

        await Task.WhenAll(concurrentTasks);

        Console.WriteLine("Cosmos DB bulk load complete");
    }
}