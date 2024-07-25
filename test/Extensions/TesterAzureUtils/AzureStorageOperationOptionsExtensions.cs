using System.Diagnostics.Tracing;
using Azure.Core;
using Azure.Data.Tables;
using Azure.Identity;
using Microsoft.Extensions.Options;
using TestExtensions;

namespace Tester.AzureUtils
{
    public static class AzureStorageOperationOptionsExtensions
    {
        private static AzureCoreEventListener _listener = new();
        public static DefaultAzureCredentialOptions Options = new DefaultAzureCredentialOptions
        {
            AdditionallyAllowedTenants = { "*" },
            Diagnostics =
            {
                LoggedHeaderNames = { "x-ms-request-id" },
                LoggedQueryParameters = { "api-version" },
                IsAccountIdentifierLoggingEnabled = true,
                IsLoggingEnabled = true,
            }
        };

        public static DefaultAzureCredential Credential = new DefaultAzureCredential(Options);

        public static Orleans.Clustering.AzureStorage.AzureStorageOperationOptions ConfigureTestDefaults(this Orleans.Clustering.AzureStorage.AzureStorageOperationOptions options)
        {
            options.TableServiceClient = GetTableServiceClient();

            return options;
        }

        public static TableServiceClient GetTableServiceClient()
        {
            var env = Environment.GetEnvironmentVariables();
            foreach (var key in env.Keys)
            {
                Console.WriteLine($"'{key}'='{env[key]}'");
            }

            Console.WriteLine($"TenantId: {Options.TenantId}");
            Console.WriteLine($"ManagedIdentityClientId: {Options.ManagedIdentityClientId}");
            Console.WriteLine($"WorkloadIdentityClientId: {Options.WorkloadIdentityClientId}");

            _ = _listener.ToString();

            return TestDefaultConfiguration.UseAadAuthentication
                ? new(TestDefaultConfiguration.TableEndpoint, Credential)
                : new(TestDefaultConfiguration.DataConnectionString);
        }

        internal sealed class AzureCoreEventListener : EventListener
        {
            protected override void OnEventSourceCreated(EventSource source)
            {
                if (source.Name.Equals("Azure-Core"))
                {
                    EnableEvents(source, EventLevel.Verbose, EventKeywords.All, new Dictionary<string, string>());
                }
            }

            protected override void OnEventWritten(EventWrittenEventArgs eventData)
            {
                Console.WriteLine($"[{eventData.EventName}] {eventData.Message} {string.Join(", ", eventData.PayloadNames.Zip(eventData.Payload).Select((k, v) => $"{k}={v}"))}.");
            }
        }
            

        public static Orleans.GrainDirectory.AzureStorage.AzureStorageOperationOptions ConfigureTestDefaults(this Orleans.GrainDirectory.AzureStorage.AzureStorageOperationOptions options)
        {
            options.TableServiceClient = GetTableServiceClient();

            return options;
        }

        public static Orleans.Persistence.AzureStorage.AzureStorageOperationOptions ConfigureTestDefaults(this Orleans.Persistence.AzureStorage.AzureStorageOperationOptions options)
        {
            options.TableServiceClient = GetTableServiceClient();

            return options;
        }

        public static Orleans.Reminders.AzureStorage.AzureStorageOperationOptions ConfigureTestDefaults(this Orleans.Reminders.AzureStorage.AzureStorageOperationOptions options)
        {
            options.TableServiceClient = GetTableServiceClient();

            return options;
        }

        public static Orleans.Configuration.AzureBlobStorageOptions ConfigureTestDefaults(this Orleans.Configuration.AzureBlobStorageOptions options)
        {
            if (TestDefaultConfiguration.UseAadAuthentication)
            {
                options.BlobServiceClient = new(TestDefaultConfiguration.DataBlobUri, Credential);
            }
            else
            {
                options.BlobServiceClient = new(TestDefaultConfiguration.DataConnectionString);
            }

            return options;
        }

        public static Orleans.Configuration.AzureQueueOptions ConfigureTestDefaults(this Orleans.Configuration.AzureQueueOptions options)
        {
            if (TestDefaultConfiguration.UseAadAuthentication)
            {
                options.QueueServiceClient = new(TestDefaultConfiguration.DataQueueUri, Credential);
            }
            else
            {
                options.QueueServiceClient = new(TestDefaultConfiguration.DataConnectionString);
            }

            return options;
        }

        public static Orleans.Configuration.AzureBlobLeaseProviderOptions ConfigureTestDefaults(this Orleans.Configuration.AzureBlobLeaseProviderOptions options)
        {
            if (TestDefaultConfiguration.UseAadAuthentication)
            {
                options.BlobServiceClient = new(TestDefaultConfiguration.DataBlobUri, Credential);
            }
            else
            {
                options.BlobServiceClient = new(TestDefaultConfiguration.DataConnectionString);
            }

            return options;
        }
    }
}
