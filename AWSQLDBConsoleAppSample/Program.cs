using Amazon.QLDB.Driver;
using Amazon.QLDB.Driver.Generic;
using Amazon.QLDB.Driver.Serialization;
using Amazon.QLDBSession;
using Amazon.Runtime;

class Program
{
    public class Person
    {
        public string FirstName { get; set; }

        public string LastName { get; set; }

        public int Age { get; set; }

        public override string ToString()
        {
            return FirstName + ", " + LastName + ", " + Age.ToString();
        }
    }

    static async Task Main(string[] args)
    {
        Console.WriteLine("Create the async QLDB driver");
        IAsyncQldbDriver driver = AsyncQldbDriver.Builder()
            .WithLedger("myTestLedger")
            .WithQLDBSessionConfig(new AmazonQLDBSessionConfig { RegionEndpoint = Amazon.RegionEndpoint.EUCentral1 })
            .WithAWSCredentials(new BasicAWSCredentials("AccessKey", "SecretKey"))
            .WithSerializer(new ObjectSerializer())
            .Build();

        Console.WriteLine("Creating the table and index");

        await driver.Execute(async txn =>
        {
            await txn.Execute("CREATE TABLE Person");
            await txn.Execute("CREATE INDEX ON Person(firstName)");
        });

        Console.WriteLine("Inserting a document");

        Person myPerson = new Person
        {
            FirstName = "John",
            LastName = "Doe",
            Age = 32
        };

        await driver.Execute(async txn =>
        {
            IQuery<Person> myQuery = txn.Query<Person>("INSERT INTO Person ?", myPerson);
            await txn.Execute(myQuery);
        });

        Console.WriteLine("Querying the table");

        // The result from driver.Execute() is buffered into memory because once the
        // transaction is committed, streaming the result is no longer possible.
        IAsyncResult<Person> selectResult = await driver.Execute(async txn =>
        {
            IQuery<Person> myQuery = txn.Query<Person>("SELECT * FROM Person WHERE FirstName = ?", "John");
            return await txn.Execute(myQuery);
        });

        await foreach (Person person in selectResult)
        {
            Console.WriteLine(person);
            // John, Doe, 32
        }

        Console.WriteLine("Updating the document");

        await driver.Execute(async txn =>
        {
            IQuery<Person> myQuery = txn.Query<Person>("UPDATE Person SET Age = ? WHERE FirstName = ?", 42, "John");
            await txn.Execute(myQuery);
        });

        Console.WriteLine("Querying the table for the updated document");

        IAsyncResult<Person> updateResult = await driver.Execute(async txn =>
        {
            IQuery<Person> myQuery = txn.Query<Person>("SELECT * FROM Person WHERE FirstName = ?", "John");
            return await txn.Execute(myQuery);
        });

        await foreach (Person person in updateResult)
        {
            Console.WriteLine(person);
            // John, Doe, 42
        }
    }
}